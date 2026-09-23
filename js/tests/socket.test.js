const { WS } = require("jest-websocket-mock")
const { createGameSocket } = require("../gamesocket.js")

// jsdom doesn't provide crypto.randomUUID
if (!globalThis.crypto) globalThis.crypto = {}
if (!globalThis.crypto.randomUUID) {
  globalThis.crypto.randomUUID = () =>
    "00000000-0000-0000-0000-000000000000"
}

const tests = {
  connectTest: [
    "client receives CONNECTION and stores socket_id",
    5000
  ],
  loginTest: [
    "client handles the LOG_IN response",
    999999
  ],
  pingTest: "server responds to PING with ACK",
  missedAck: [
    "client sulks when server fails to ACK",
    5000
  ]
}
const run = [
  connectTest,
  loginTest,
  pingTest,
  missedAck
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
const SOCKET_ID  = "test_socket_id"

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

beforeEach(async () => {
  server = new WS(SERVER_URL)
  client = createGameSocket({
    url: "ws://localhost:1234",
    path: "/ws",
  })

  events.forEach(event => client.on(event, treatEvent))

  client.connect()
  await server.connected

  // Handshake: server tells the client its socket_id
  server.send(JSON.stringify({
    sender_id: "SYSTEM",
    subject: "CONNECTION",
    recipient_id: SOCKET_ID,
  }))
  await pause()
})

afterEach(() => {
  client.disconnect()
  WS.clean()
})


async function connectTest(title) {
  console.log(`******** ${title.toUpperCase()} ********`)
  // The socketId is not directly exposed, but the client will only
  // send user messages once it has one. Send a message and check
  // that the server sees sender_id === SOCKET_ID.
  client.send({
    recipient_id: "SYSTEM",
    subject: "LOG_IN",
    user_name: "probe",
  }, 1000) // no rsvp

  const raw = await server.nextMessage
  const outbound = JSON.parse(raw)

  expect(outbound.sender_id).toBe(SOCKET_ID)

  // Server must respond with ACK
  server.send(JSON.stringify({
    subject: "ACK",
    corr: outbound.corr,
  }))

  await pause()
}


async function loginTest(title){
  console.log(`******** ${title.toUpperCase()} ********`)
  const promise = client.send({
    recipient_id: "SYSTEM",
    subject: "LOG_IN",
    user_name: "test_user_name",
  }, 2000) // requires rsvp
  .then(result => {
    console.log("result", JSON.stringify(result, null, '  '))
  })

  const raw = await server.nextMessage
  const login = JSON.parse(raw)
  expect(login.subject).toBe("LOG_IN")
  expect(login.user_name).toBe("test_user_name")

  // Server must respond with ACK
  server.send(JSON.stringify({
    subject: "ACK",
    was: "LOG_IN",
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
    user_id: "test_user_id",
  }))
  await pause()

  expect(client.getUser()).toEqual({
    user_name: "test_user_name",
    user_id: "test_user_id",
  })
}


async function pingTest(title) {
  console.log(`******** ${title.toUpperCase()} ********`)
  client.send({
    subject: "PING"
  })

  const raw = await server.nextMessage
  const incoming = JSON.parse(raw)
  expect(incoming.sender_id).toBe(SOCKET_ID)
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
  expect(incoming.sender_id).toBe(SOCKET_ID)
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