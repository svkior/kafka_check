require('./style.css');
import KefirReact from 'kefir-react';
import Kefir from 'kefir';
import React from 'react';
import ReactDOM from 'react-dom';
import {messageStream} from './lib/wswork';

messageStream.onValue( (v) => {
  console.log('Got from message Stream:', v);
});


const userProperty = Kefir.fromPoll(500, () => ({
        name: 'Current time is: ' + (new Date()).toLocaleTimeString()
    }))
    .toProperty(() => ({
        name: 'Current Time'
    }));

class App extends React.Component {
  render(){
    return <span>{this.props.user.name}</span>;
  }
}

class Main extends React.Component {
  render() {
    return React.createElement(KefirReact, {
      streams: {
        user: userProperty
      },
      render: values => React.createElement(App, values)
    })
  }
}

ReactDOM.render(<Main/>, document.getElementById('app'));
