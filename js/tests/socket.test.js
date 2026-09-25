const { WS } = require("jest-websocket-mock")
const { createGameSocket } = require("../gameSocket.js")

// jsdom doesn't provide crypto.randomUUID
if (!globalThis.crypto) globalThis.crypto = {}
if (!globalThis.crypto.randomUUID) {
  globalThis.crypto.randomUUID = () =>
    "00000000-0000-0000-0000-000000000000"
}

const tests = {
  connectTest: "client receives CONNECTION and stores socket_id",
  loginTest: [
    "client handles the LOGGED_IN response",
    10000
  ],
  loginFailTest: [
    "client handles the LOGIN_FAILED response",
    10000
  ],
  loginNoACKTest: [
    "client handles the LOGGED_IN with no ACK",
    600_000
  ],
  pingTest: "server responds to PING with ACK",
  missedAck: "client absorbs one missing ACK",
  twoMissedAcks: "client sulks when server fails to ACK",
  missedActionAck: [
    "action message resent if a single ACK missed"
  ],
  missedAcksForceReconnect: [
    "client reconnects after two missed ACKs",
    120_000
  ],
}
const run = [
  // connectTest,
  // loginTest,
  // loginFailTest,
  // loginNoACKTest,
  // pingTest,
  // missedAck,
  // // twoMissedAcks,
  // missedActionAck,
  missedAcksForceReconnect
]

run.forEach(trial => {
  const details = tests[trial.name]
  const [title, timeout] = (Array.isArray(details))
    ?  details
    : [details, 5000]
  test(trial.name, () => trial(title), timeout)
})

const pause = (delay) => new Promise(
  resolve => setTimeout(resolve, delay || 0)
)

const SERVER_URL = "ws://localhost:1234/ws"
const USER_ID = "test_user_id"
let socket_id // "SOCKET_n"


const events = [
  "open",
  "close",
  "incoming",
  "pending",
  "state",
  "error",
  "warn",
  "info",
]
const alerts = {}
function treatEvent(payload, event) {
  const topic = alerts[event] || (alerts[event] = [])
  topic.push(payload)
  // console.log(event, payload)
}
let server
let client
let counter = 0

beforeEach(async () => {
  server = new WS(SERVER_URL)
  client = createGameSocket({
    url: "ws://localhost:1234",
    path: "/ws",
  })

  events.forEach(event => client.on(event, treatEvent))

  client.connect()
  // handshake(server)

  await server.connected

  // Handshake: server tells the client its socket_id
  socket_id = `SOCKET_${++counter}`
  server.send(JSON.stringify({
    sender_id: "SYSTEM",
    subject: "CONNECTION",
    recipient_id: socket_id
  }))
  await pause()
})

afterEach(() => {
  client.disconnect()
  WS.clean()
})


async function handshake() {
  await server.connected

  // Handshake: server tells the client its socket_id
  socket_id = `SOCKET_${++counter}`
  // console.log("socket_id:", socket_id)
  server.send(JSON.stringify({
    sender_id: "SYSTEM",
    subject: "CONNECTION",
    recipient_id: socket_id
  }))
  await pause()
}


async function connectTest(title) {
  console.log(`******** ${title.toUpperCase()} ********`)
  // The socketId is not directly exposed, but the client will only
  // send user messages once it has one. Send a message and check
  // that the server sees sender_id === socket_id.
  client.send({
    recipient_id: "SYSTEM",
    subject: "LOG_IN",
    user_name: "probe",
  }, 1000) // no rsvp

  const raw = await server.nextMessage
  const outbound = JSON.parse(raw)

  expect(outbound.sender_id).toBe(socket_id)

  // Server must respond with ACK
  server.send(JSON.stringify({
    subject: "ACK",
    corr: outbound.corr,
  }))

  await pause()
}


async function loginTest(title){
  console.log(`******** ${title.toUpperCase()} ********`)
  client.send({
    recipient_id: "SYSTEM",
    subject: "LOG_IN",
    user_name: "test_user_name",
  }, 2000) // requires rsvp
  .then(result => {
    // console.log(JSON.stringify(result, null, '  '))
    // {
    //   "status": "LOGGED_IN",
    //   "message": {
    //     "corr": "93ba8575-2973-4b00-9c95-af2a70c021c4",
    //     "recipient_id": "SYSTEM",
    //     "subject": "LOG_IN",
    //     "user_name": "test_user_name",
    //     "sender_id": "test_socket_id"
    //   }
    // }
    expect(result.status).toBe("LOGGED_IN")
  })

  const raw = await server.nextMessage
  const login = JSON.parse(raw)
  expect(login.subject).toBe("LOG_IN")
  expect(login.user_name).toBe("test_user_name")
  expect(login.sender_id).toBe(socket_id) // proof that it was set

  // Server must respond with ACK
  server.send(JSON.stringify({
    subject: "ACK",
    corr: login.corr,
  }))
  await pause()

  // Server confirms login
  server.send(JSON.stringify({
    sender_id: "SYSTEM",
    subject: "LOG_IN",
    corr: login.corr,
    status: "LOGGED_IN",
    user_name: login.user_name,
    user_id: USER_ID,
  }))
  await pause()

  expect(client.getUser()).toEqual({
    user_name: "test_user_name",
    user_id: USER_ID,
  })

  console.log("############ finish loginTest ############")
}


async function loginFailTest(title){
  console.log(`******** ${title.toUpperCase()} ********`)
  client.send({
    recipient_id: "SYSTEM",
    subject: "LOG_IN",
    user_name: "test_user_name",
  }, 2000) // requires rsvp
  .then(result => {
    // console.log(JSON.stringify(result, null, '  '))
    // {
    //   "status": "LOGIN_FAILED",
    //   "message": {
    //     "corr": "93ba8575-2973-4b00-9c95-af2a70c021c4",
    //     "recipient_id": "SYSTEM",
    //     "subject": "LOG_IN",
    //     "user_name": "test_user_name",
    //     "sender_id": "test_socket_id"
    //   }
    // }
    expect(result.status).toBe("LOGIN_FAILED")
  })

  const raw = await server.nextMessage
  const login = JSON.parse(raw)
  expect(login.subject).toBe("LOG_IN")
  expect(login.user_name).toBe("test_user_name")

  // Server must respond with ACK
  server.send(JSON.stringify({
    subject: "ACK",
    corr: login.corr,
  }))
  await pause()

  // Server confirms login
  server.send(JSON.stringify({
    sender_id: "SYSTEM",
    subject: "LOG_IN",
    corr: login.corr,
    status: "LOGIN_FAILED",
    user_name: login.user_name,
    user_id: USER_ID,
  }))
  await pause()

  const result = alerts.pending.pop()
  expect(result.status).toBe("handled")
  expect(result.message.status).toBe("LOGIN_FAILED")

  // console.log(JSON.stringify(result, null, '  '))
  // {
  //   "corr": "bf7d9980-b257-4df2-aee8-f95f209fc9b5",
  //   "message": {
  //     "sender_id": "SYSTEM",
  //     "subject": "LOG_IN",
  //     "corr": "bf7d9980-b257-4df2-aee8-f95f209fc9b5",
  //     "status": "LOGIN_FAILED",
  //     "user_name": "test_user_name",
  //     "user_id": USER_ID
  //   },
  //   "status": "handled"
  // }

  expect(client.getUser()).toEqual({
    user_name: "",
    user_id: "",
  })
}


async function loginNoACKTest(title){
  console.log(`******** ${title.toUpperCase()} ********`)
  client.send({
    recipient_id: "SYSTEM",
    subject: "LOG_IN",
    user_name: "test_user_name",
  }, 2000) // requires rsvp
  .then(result => {
    // console.log(JSON.stringify(result, null, '  '))
    // {
    //   "status": "LOGGED_IN",
    //   "message": {
    //     "corr": "93ba8575-2973-4b00-9c95-af2a70c021c4",
    //     "recipient_id": "SYSTEM",
    //     "subject": "LOG_IN",
    //     "user_name": "test_user_name",
    //     "sender_id": "test_socket_id"
    //   }
    // }
    expect(result.status).toBe("LOGGED_IN")
  })

  const raw = await server.nextMessage
  const login = JSON.parse(raw)
  expect(login.subject).toBe("LOG_IN")
  expect(login.user_name).toBe("test_user_name")
  expect(login.sender_id).toBe(socket_id) // proof that it was set

  // Server fails to send ACK...
  // server.send(JSON.stringify({
  //   subject: "ACK",
  //   corr: login.corr,
  // }))
  // await pause()

  // ... but still confirms login
  server.send(JSON.stringify({
    sender_id: "SYSTEM",
    subject: "LOG_IN",
    corr: login.corr,
    status: "LOGGED_IN",
    user_name: login.user_name,
    user_id: USER_ID,
  }))
  await pause()

  expect(client.getUser()).toEqual({
    user_name: "test_user_name",
    user_id: USER_ID,
  })
}


async function pingTest(title) {
  console.log(`******** ${title.toUpperCase()} ********`)
  client.send({
    subject: "PING"
  })

  const raw = await server.nextMessage
  const incoming = JSON.parse(raw)
  expect(incoming.sender_id).toBe(socket_id)
  expect(incoming.corr).toMatch(/^[0-9a-f-]{36}$/)

  // Server responds with ACK
  server.send(JSON.stringify({
    subject: "ACK",
    recipient_id: incoming.sender_id,
    corr: incoming.corr
  }))
  await pause()

  expect(alerts.info.pop()).toMatch(/ACK \d+, corr: [a-f0-9-]{8}/)
}


async function missedAck(title) {
  console.log(`******** ${title.toUpperCase()} ********`)
  client.send({
    subject: "PING"
  })

  const raw = await server.nextMessage
  const incoming = JSON.parse(raw)
  expect(incoming.sender_id).toBe(socket_id)
  expect(incoming.corr).toMatch(/^[0-9a-f-]{36}$/)

  // Server responds late with ACK
  const was = Date.now()
  setTimeout(() => {
    const delay = Date.now() - was
    // console.log("delay:", delay)
    server.send(JSON.stringify({
      subject: "ACK",
      recipient_id: incoming.sender_id,
      corr: incoming.corr,
      delay
    }))
  }, 1500)
  await pause(1500)

  expect(alerts.warn?.pop() || "").toMatch(
    /MISSED ACK: [a-f0-9-]{8} after \d+ ms/
  )
}


async function twoMissedAcks(title) {
  console.log(`******** ${title.toUpperCase()} ********`)
}


async function missedActionAck(title) {
  console.log(`******** ${title.toUpperCase()} ********`)
  client.send({
    subject: "MOVE"
  })

  const raw = await server.nextMessage
  const incoming = JSON.parse(raw)
  const raw_corr = incoming.corr
  expect(incoming.sender_id).toBe(socket_id)
  expect(raw_corr).toMatch(/^[0-9a-f-]{36}$/)
  // console.log("incoming", JSON.stringify(incoming, null, '  '))

  // Server responds late with ACK
  const was = Date.now()
  setTimeout(() => {
    const delay = Date.now() - was
    // console.log("delay:", delay)
    server.send(JSON.stringify({
      subject: "ACK",
      recipient_id: incoming.sender_id,
      corr: incoming.corr,
      delay
    }))
  }, 1500)
  await pause(1500)

  expect(alerts.warn?.pop() || "").toMatch(
    /MISSED ACK: [a-f0-9-]{8} after \d+ ms/
  )

  const resend = await server.nextMessage
  const resent = JSON.parse(resend)
  // console.log("resent", JSON.stringify(resent, null, '  '))

  expect(resent.sender_id).toBe(socket_id)
  expect(resent.corr).toBe(raw_corr)
}


async function missedAcksForceReconnect(title) {
  console.log(`******** ${title.toUpperCase()} ********`)
  // loginTest("Logging in...")
  console.log("############ start loginTest ############")

  client.send({
    recipient_id: "SYSTEM",
    subject: "LOG_IN",
    user_name: "test_user_name",
  }, 2000) // requires rsvp
  .then(result => {
    // console.log(JSON.stringify(result, null, '  '))
    // {
    //   "status": "LOGGED_IN",
    //   "message": {
    //     "corr": "93ba8575-2973-4b00-9c95-af2a70c021c4",
    //     "recipient_id": "SYSTEM",
    //     "subject": "LOG_IN",
    //     "user_name": "test_user_name",
    //     "sender_id": "test_socket_id"
    //   }
    // }
    expect(result.status).toBe("LOGGED_IN")
  })

  const raw = await server.nextMessage
  const login = JSON.parse(raw)
  expect(login.subject).toBe("LOG_IN")
  expect(login.user_name).toBe("test_user_name")
  expect(login.sender_id).toBe(socket_id) // proof that it was set

  // Server must respond with ACK
  server.send(JSON.stringify({
    subject: "ACK",
    corr: login.corr,
  }))
  await pause()

  // Server confirms login
  server.send(JSON.stringify({
    sender_id: "SYSTEM",
    subject: "LOG_IN",
    corr: login.corr,
    status: "LOGGED_IN",
    user_name: login.user_name,
    user_id: USER_ID,
  }))
  await pause()

  expect(client.getUser()).toEqual({
    user_name: "test_user_name",
    user_id: USER_ID,
  })

  console.log("############ finish loginTest ############")

  // First PING will receive no ACK
  let alert = alerts.info.findLast( alert => (
    alert.generation
  ))
  expect(alert.generation).toBe(1)

  client.send({
    subject: "PING"
  })
  .then(result => {
    console.log("MISSED PING result:", result)
    expect(result).toBe("clean up PING")
  })

  const incoming1 = await server.nextMessage
  console.log("incoming1:", incoming1)
  const ping1 = JSON.parse(incoming1)
  const corr1 = ping1.corr
  console.log("ping1", JSON.stringify(ping1, null, '  '));
  expect(ping1.subject).toBe("PING")
  expect(corr1).toMatch(/^[0-9a-f-]{36}$/)

  // Server fails to deliver any ACK; ackTimer fires
  await pause(1500)

  const warn = alerts.warn?.slice(-1)[0] || ""
  const regex = new RegExp(`MISSED ACK: ${corr1.slice(0,8)} after \\d+ ms`)
  console.log(`${regex}.test(\n  "${warn}"\n)`)
  expect(alerts.warn?.pop() || "").toMatch(regex)

  // Send a second PING (don't wait for the next IDLE PING)
  client.send({
    subject: "PING"
  })

  // Check that this was sent by the same socket
  console.log("generation:", alerts.info.slice(-1)[0].generation)

  const incoming2 = await server.nextMessage
  const ping2 = JSON.parse(incoming2)
  const corr2 = ping2.corr
  console.log("ping2", JSON.stringify(ping2, null, '  '));

  expect(ping2.subject).toBe("PING")
  expect(corr2).toMatch(/^[0-9a-f-]{36}$/)
  expect(corr1 === corr2).toBeFalsy()
  await pause(1500)

  expect(alerts.warn?.pop() || "").toMatch(
    new RegExp(`MISSED ACK: ${corr2.slice(0,8)} after \\d+ ms`)
  )

  // Server fails to deliver ACK again; ackTimer fires a 2nd time.
  // Client should _abandonSocket() which calls socket.close()...

  alert = alerts.close.slice(-1)[0]
  console.log("close", JSON.stringify(alert, null, '  '));
  await pause(300)

  // ...and _rescheduleConnection() which calls _openSocket()
  // after 250ms

  await server.connected
  console.log(("*********** RECONNECTED ***********"))
  handshake()

  // console.log("alerts.open", JSON.stringify(alerts.open, null, '  '));

  alert = alerts.open.slice(-1)[0]
  // console.log("open", JSON.stringify(alert, null, '  '));

  // // Client should now _identifyUser()
  const reconnect = await server.nextMessage
  const request = JSON.parse(reconnect)
  expect(request.sender_id).toBe("SOCKET_2")
  expect(request.user_id).toBe(USER_ID)

  // Server must respond with ACK
  server.send(JSON.stringify({
    subject: "ACK",
    corr: request.corr,
  }))
  await pause()

  // Server confirms login
  server.send(JSON.stringify({
    sender_id: "SYSTEM",
    subject: "LOG_IN",
    corr: request.corr,
    status: "LOGGED_IN",
    user_name: request.user_name,
    user_id: USER_ID,
  }))
  await pause()

  expect(client.getUser()).toEqual({
    user_name: request.user_name,
    user_id: USER_ID,
  })
}


///////

function sendLateACK(incoming, lateMs) {
  // Server responds late with ACK
  const was = Date.now()
  setTimeout(() => {
    const delay = Date.now() - was
    // console.log("delay:", delay)
    server.send(JSON.stringify({
      subject: "ACK",
      recipient_id: incoming.sender_id,
      corr: incoming.corr,
      delay
    }))
  }, lateMs)
}