# GameSocket MVP #

Demo of a resilient WebSocket client that is designed to handle frequent connection disruptions, such as those encountered when connecting from within Russia to a backend WebSocket server outside Russia, with the help of a VPN.

The client is designed to work in two different states:
* IDLE
* EAGER

Both states send regular PING messages to the backend: IDLE about twice a minute, EAGER about once a second. For turn-based games, a player may be waiting for input from another player which may come at any time. The EAGER setting ensures that the connection is open for incoming messages... except when it is closed.

The IDLE setting relies on outgoing messages to ensure that the connection remains open. If an outgoing message fails to be sent, the current socket is torn down, and a new one is created, and used to resend the message. This is appropriate when it is the current client's turn to play, and incoming messages are not critical.

## GameSocket

The global GameSocket object exposes a `createGameSocket()` function, which returns an API object:

```js
{
  // Stay connected
  cpr,
  setURL,
  connect,
  disconnect,
  isConnected, // returns boolean
  // Send a message
  send,
  // Switch between states
  beEager,
  beIdle,
  getState: () => state,
  // ID of logged in user
  getUser: () => ({ user_name, user_id }),
  // Listening for events
  on,
  off,
  events: Object.keys(listeners) // array
}
```

These functions and properties can be used interact with the WebSocket.

## Why WebSocket and not Socket.io?

Under certain test conditions, Socket.io fails to connect at all. This is probably due to its more complex handshake which gives Deep Packet Inspection more reasons to break the incipient connection.