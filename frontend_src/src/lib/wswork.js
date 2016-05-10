

import Kefir from 'kefir';

let socket = new WebSocket(`ws://${location.hostname}:8090/device`);


export const messageStream = Kefir.stream( (emitter) => {

  socket.onopen = () => {
    emitter.emit({
      type: 'ws_opened'
    });
  };

  socket.onclose = () => {
    emitter.emit({
      type: 'ws_closed'
    });
  }

  socket.onmessage = (e) => {
    console.log(e.data);
    let msg = JSON.parse(e.data);
    emitter.emit(msg);
  };

  socket.onerror = (e) => {
    console.log(e);
    emitter.emit({
      type: 'error',
      data: e
    })
  }

});
