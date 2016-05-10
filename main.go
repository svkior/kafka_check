package main

import (
  "crypto/tls"
  "crypto/x509"
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
  "flag"
  "github.com/Shopify/sarama"
  "os"
  "strings"
  "net/http"
  "time"
)

var  (
  addr      = flag.String("addr", ":8090", "This is the Address to bind to")
  // MAY BE o
  brokers   = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
  verbose   = flag.Bool("verbose", false, "Turn on Sarama logging")
  certFile  = flag.String("certificate", "", "The optional certificate file for client authentication")
  keyFile   = flag.String("key", "", "The optional certificate authority file for TLS client authentication")
  caFile    = flag.String("ca", "", "The optional certificate authority file for TLS client authentication")
  verifySsl = flag.Bool("verify", false, "Optional verify ssl certificates chain")
)

func main(){

  flag.Parse()

  if *verbose {
    sarama.Logger = log.New(os.Stdout, "[sarama]", log.LstdFlags)
  }

  if *brokers == "" {
    flag.PrintDefaults()
    os.Exit(1)
  }

  brokerList := strings.Split(*brokers, ",")

  server := &Server{
    DataCollector: newDataCollector(brokerList),
    AccessLogProducer: newAccessLogProducer(brokerList),
    Consumer: newConumer(brokerList),
  }
  defer func(){
    if err := server.Close(); err != nil {
      log.Println("Failed to close server", err)
    }
  }()

  log.Fatal(server.Run(*addr))
}

func createTlsConfiguration() (t *tls.Config){
  if *certFile != "" && *keyFile != "" && *caFile != "" {
    cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
    if err != nil {
      log.Fatal(err)
    }

    caCert, err := ioutil.ReadFile(*caFile)
    if err != nil {
      log.Fatal(err)
    }

    caCertPool := x509.NewCertPool()
    caCertPool.AppendCertsFromPEM(caCert)

    t = &tls.Config{
      Certificates:       []tls.Certificate{cert},
      RootCAs:            caCertPool,
      InsecureSkipVerify: *verifySsl,
    }
  }

  return t // will be nil by default if nothing is provided
}

type Server struct {
  DataCollector       sarama.SyncProducer
  AccessLogProducer   sarama.AsyncProducer
  Consumer            sarama.Consumer
}

func (s *Server) Close() error {

  if err := s.Consumer.Close(); err != nil {
    log.Println("Failed to shut down Consumer cleanly", err)
  }

  if err := s.DataCollector.Close(); err != nil {
    log.Println("Failed to shut down data collector cleanly", err)
  }

  if err := s.AccessLogProducer.Close(); err != nil {
    log.Println("Failed to shut down access log producer cleanly", err)
  }

  return nil
}


func (s *Server) OldHandler() http.Handler {
  return s.withAccessLog(s.collectQueryStringData())
}

func (s *Server) Run(addr string) error {

  http.Handle("/", http.FileServer(http.Dir("./build")))
  http.Handle("/device", s)
  log.Printf("Listening for requests on %s...\n", addr)
  return http.ListenAndServe(addr, nil)
}

func (s *Server) collectQueryStringData() http.Handler {
  return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request){
    if r.URL.Path != "/"{
      http.NotFound(w, r)
      return
    }

    partition, offset, err := s.DataCollector.SendMessage(&sarama.ProducerMessage{
      Topic: "important",
      Value: sarama.StringEncoder(r.URL.RawQuery),
    })

    if err != nil {
      w.WriteHeader(http.StatusInternalServerError)
      fmt.Fprintf(w, "Failed to store your data: %s", err)
    } else {
      fmt.Fprintf(w, "Your data is stored with unique identifier important/%d%d", partition, offset)
    }
  })
}

type accessLogEntry struct {
  Method        string  `json:"method"`
  Host          string  `json:"host"`
  Path          string  `json:"path"`
  IP            string  `json:"ip"`
  ResponseTime  float64 `json:"response_time"`

  encoded []byte
  err     error
}

func (ale *accessLogEntry) ensureEncoded() {
  if ale.encoded == nil && ale.err == nil {
    ale.encoded, ale.err = json.Marshal(ale)
  }
}

func (ale *accessLogEntry) Length() int {
  ale.ensureEncoded()
  return len(ale.encoded)
}

func (ale *accessLogEntry) Encode() ([]byte, error){
  ale.ensureEncoded()
  return ale.encoded, ale.err
}


func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request){

/*  socket, err := upgrader.Upgrade(w, req, nil)
  if err != nil {
    log.Fatal("DeviceHandler:", err)
  }
  */
/*
  client := NewRemoteClient(socket)
  d.AddElement(client)
  client.Read()
  */
}

func (s *Server) withAccessLog(next http.Handler) http.Handler {

  return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request){
    started := time.Now()
    next.ServeHTTP(w, r)

    entry := &accessLogEntry{
      Method:   r.Method,
      Host:     r.Host,
      Path:     r.RequestURI,
      IP:       r.RemoteAddr,
      ResponseTime: float64(time.Since(started))/ float64(time.Second),
    }

    s.AccessLogProducer.Input() <- &sarama.ProducerMessage{
      Topic: "access_log",
      Key:    sarama.StringEncoder(r.RemoteAddr),
      Value:  entry,
    }
  })
}

func newDataCollector(brokerList []string) sarama.SyncProducer {

  config := sarama.NewConfig()
  config.Producer.RequiredAcks = sarama.WaitForAll
  config.Producer.Retry.Max = 10
  tlsConfig := createTlsConfiguration()
  if tlsConfig != nil {
    config.Net.TLS.Config = tlsConfig
    config.Net.TLS.Enable = true
  }

  producer, err := sarama.NewSyncProducer(brokerList, config)
  if err != nil {
    log.Fatalln("Failed to start Sarama producer:", err)
  }

  return producer
}


func newAccessLogProducer(brokerList []string) sarama.AsyncProducer {

  config := sarama.NewConfig()
  tlsConfig := createTlsConfiguration()
  if tlsConfig != nil {
    config.Net.TLS.Enable = true
    config.Net.TLS.Config = tlsConfig
  }

  config.Producer.RequiredAcks = sarama.WaitForLocal
  config.Producer.Compression = sarama.CompressionSnappy
  config.Producer.Flush.Frequency = 500 * time.Millisecond

  producer, err := sarama.NewAsyncProducer(brokerList, config)
  if err != nil {
    log.Fatalln("Failed to start Sarama producer:", err)
  }

  go func() {
    for err := range producer.Errors(){
      log.Println("Failed to write access log entry:", err)
    }
  }()

  return producer
}

func newConumer(brokerList []string) sarama.Consumer {
  consumer, err := sarama.NewConsumer(brokerList, nil) // TODO: Create right config
  if err != nil {
    log.Fatalln("Error creating new consumer")
  }

  partititonConsumer, err := consumer.ConsumePartition("access_log", 0, sarama.OffsetNewest)

  if err != nil {
    panic(err)
  }

  go func(){
    for {
      msg := <-partititonConsumer.Messages()
      log.Printf("Consumed message offset %d, Value: %s", msg.Offset, msg.Value)
    }
  }()

  return consumer
}
