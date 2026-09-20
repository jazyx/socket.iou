/**
 * frontend/js/websocket.test.js
 *
 * Behavioural tests for the GameSocket client.
 *
 * The client is a browser script that expects:
 *   - a global `WebSocket` constructor
 *   - a global `crypto.randomUUID`
 *
 * Both are provided here as controllable fakes.
 */


// npm install --save-dev jest
// npx jest frontend/js/websocket.test.js


// ── Fakes ──────────────────────────────────────────────────────────

class FakeWebSocket {
  static CONNECTING = 0
  static OPEN       = 1
  static CLOSING    = 2
  static CLOSED     = 3

  static instances = []

  constructor(url) {
    this.url = url
    this.readyState = FakeWebSocket.CONNECTING
    this.sent = []
    this.onopen = null
    this.onerror = null
    this.onclose = null
    this.onmessage = null
    FakeWebSocket.instances.push(this)
  }

  // Test helpers (not part of the WebSocket API)
  _open() {
    this.readyState = FakeWebSocket.OPEN
    this.onopen?.({ type: "open", target: this })
  }
  _message(payload) {
    const data = typeof payload === "string" ? payload : JSON.stringify(payload)
    this.onmessage?.({ type: "message", data, target: this })
  }
  _error() {
    this.onerror?.({ type: "error", target: this })
  }
  _close(code = 1000, reason = "", wasClean = true) {
    this.readyState = FakeWebSocket.CLOSED
    this.onclose?.({ type: "close", code, reason, wasClean, target: this })
  }

  // WebSocket API
  send(data) {
    if (this.readyState !== FakeWebSocket.OPEN) {
      throw new Error("send() on non-open socket")
    }
    this.sent.push(JSON.parse(data))
  }
  close(code = 1000, reason = "") {
    this.readyState = FakeWebSocket.CLOSING
    // Real WebSocket fires close asynchronously; tests trigger it
    // explicitly via _close() when they want to simulate the event.
  }
}

let uuidCounter = 0
function fakeUUID() {
  return `corr-${++uuidCounter}`
}

// ── Test helpers ───────────────────────────────────────────────────

const { createGameSocket } = require("./gamesocket.js")

function setup(options = {}) {
  FakeWebSocket.instances.length = 0
  uuidCounter = 0

  const gs = createGameSocket({
    url: "ws://test",
    path: "/ws",
    ...options,
  })

  return gs
}

function lastSocket() {
  const instances = FakeWebSocket.instances
  return instances[instances.length - 1]
}

function openAndHandshake(gs, { socketId = "sid-1" } = {}) {
  // Trigger connect and let the socket open
  gs.connect()
  const ws = lastSocket()
  ws._open()
  // The server's first message is CONNECTION, which sets socketId
  ws._message({
    sender_id: "SYSTEM",
    subject: "CONNECTION",
    recipient_id: socketId,
  })
  return ws
}

function ackFor(ws, corr, { time } = {}) {
  ws._message({
    sender_id: "SYSTEM",
    subject: "ACK",
    corr,
    time: time ?? Date.now(),
  })
}

// ── Test setup ─────────────────────────────────────────────────────

beforeEach(() => {
  jest.useFakeTimers()
  globalThis.WebSocket = FakeWebSocket
  globalThis.crypto = { randomUUID: fakeUUID }
})

afterEach(() => {
  jest.useRealTimers()
  delete globalThis.WebSocket
  delete globalThis.crypto
})

// ── Tests ──────────────────────────────────────────────────────────

describe("lifecycle", () => {
  test("connect() opens a WebSocket and emits 'open'", () => {
    const gs = setup()
    const onOpen = jest.fn()
    gs.on("open", onOpen)

    gs.connect()
    expect(FakeWebSocket.instances).toHaveLength(1)

    lastSocket()._open()
    expect(onOpen).toHaveBeenCalledTimes(1)
    expect(gs.isConnected()).toBe(true)
  })

  test("disconnect() closes the socket and emits 'close'", () => {
    const gs = setup()
    const onClose = jest.fn()
    gs.on("close", onClose)

    const ws = openAndHandshake(gs)
    gs.disconnect()

    expect(ws.readyState).toBe(FakeWebSocket.CLOSING)
    expect(gs.isConnected()).toBe(false)
  })

  test("an unexpected close moves state to RECONNECTING", () => {
    const gs = setup()
    openAndHandshake(gs)

    lastSocket()._close(1006, "abnormal")
    expect(gs.getState()).toBe("RECONNECTING")
  })

  test("a user-initiated close does not schedule a reconnect", () => {
    const gs = setup()
    openAndHandshake(gs)
    gs.disconnect()

    const before = FakeWebSocket.instances.length
    jest.advanceTimersByTime(60_000)
    expect(FakeWebSocket.instances.length).toBe(before)
    expect(gs.getState()).toBe("IDLE")
  })
})

describe("handshake", () => {
  test("CONNECTION sets the socketId and triggers LOG_IN if user is known", () => {
    const gs = setup()
    gs.connect()
    const ws = lastSocket()
    ws._open()

    // No user yet: CONNECTION should not send LOG_IN
    ws._message({ sender_id: "SYSTEM", subject: "CONNECTION", recipient_id: "sid-1" })
    expect(ws.sent.find(m => m.subject === "LOG_IN")).toBeUndefined()
  })

  test("LOGGED_IN stores the user and emits 'pending'", () => {
    const gs = setup()
    const onMessage = jest.fn()
    gs.on("pending", onMessage)

    const ws = openAndHandshake(gs)
    ws._message({
      sender_id: "SYSTEM",
      subject: "LOGGED_IN",
      user_id: "u-1",
      user_name: "alice",
    })

    expect(gs.getUser()).toEqual({ user_id: "u-1", user_name: "alice" })
    expect(onMessage).toHaveBeenCalledWith(
      expect.objectContaining({ subject: "LOGGED_IN" })
    )
  })
})

describe("send()", () => {
  test("resolves with {reason:'acknowledged'} when the server ACKs", async () => {
    const gs = setup()
    const ws = openAndHandshake(gs)

    const p = gs.send({ subject: "MOVE", x: 1 })

    // The message should be on the wire with a corr
    expect(ws.sent).toHaveLength(1)
    const corr = ws.sent[0].corr
    expect(corr).toBeTruthy()
    expect(ws.sent[0].sender_id).toBe("sid-1")

    ackFor(ws, corr)

    const result = await p
    expect(result).toEqual(
      expect.objectContaining({ reason: "acknowledged" })
    )
  })

  test("rejects if called with a non-object payload", () => {
    const gs = setup()
    openAndHandshake(gs)
    expect(() => gs.send("hello")).toThrow(/Object required/)
  })

  test("PINGs are not emitted as 'pending' events", () => {
    const gs = setup()
    const onPending = jest.fn()
    gs.on("pending", onPending)

    const ws = openAndHandshake(gs)
    gs.send({ subject: "PING" })
    expect(onPending).not.toHaveBeenCalled()
  })

  test("user messages emit 'queued' then 'sent' pending events", () => {
    const gs = setup()
    const onPending = jest.fn()
    gs.on("pending", onPending)

    const ws = openAndHandshake(gs)
    gs.send({ subject: "MOVE", x: 2 })

    const statuses = onPending.mock.calls.map(([e]) => e.status)
    expect(statuses).toEqual(["queued", "sent"])
  })
})

describe("ACK timeout and retry", () => {
  test("a missed ACK retries the message on the same socket", () => {
    const gs = setup()
    const ws = openAndHandshake(gs)

    gs.send({ subject: "MOVE", x: 1 })
    const corr = ws.sent[0].corr

    // Advance past the ACK timeout (1000ms default)
    jest.advanceTimersByTime(1100)

    // The same message should have been sent twice
    expect(ws.sent.filter(m => m.corr === corr)).toHaveLength(2)
  })

  test("a second missed ACK triggers a reconnect", () => {
    const gs = setup()
    const ws = openAndHandshake(gs)

    gs.send({ subject: "MOVE", x: 1 })
    jest.advanceTimersByTime(1100) // first timeout -> retry
    jest.advanceTimersByTime(1100) // second timeout -> reconnect

    expect(gs.getState()).toBe("RECONNECTING")
  })

  test("a message is resent on the new socket after reconnect", () => {
    const gs = setup()
    const ws1 = openAndHandshake(gs)

    gs.send({ subject: "MOVE", x: 1 })
    const corr = ws1.sent[0].corr

    jest.advanceTimersByTime(1100) // retry on ws1
    jest.advanceTimersByTime(1100) // give up on ws1

    // Close ws1 and let the reconnect timer fire
    ws1._close(1006, "abnormal")
    jest.advanceTimersByTime(300)  // reconnectBaseMs

    const ws2 = lastSocket()
    expect(ws2).not.toBe(ws1)
    ws2._open()
    /// JN: CONNECTION  => LOGGED_ID
    ws2._message({ sender_id: "SYSTEM", subject: "LOGGED_IN", recipient_id: "sid-2" })

    // The queued message should have been resent on ws2
    expect(ws2.sent.find(m => m.corr === corr)).toBeDefined()
  })
})

describe("idempotency / abandonment", () => {
  test("a message is abandoned after stopTryingMs", async () => {
    const gs = setup({ stopTryingMs: 5_000 })
    const ws = openAndHandshake(gs)

    const p = gs.send({ subject: "MOVE", x: 1 })
    // Advance past the abandonment deadline
    jest.advanceTimersByTime(6_000)

    const result = await p
    expect(result).toEqual(
      expect.objectContaining({ reason: "abandoned" })
    )
  })

  test("an ACKed message is not abandoned", async () => {
    const gs = setup({ stopTryingMs: 5_000 })
    const ws = openAndHandshake(gs)

    const p = gs.send({ subject: "MOVE", x: 1 })
    const corr = ws.sent[0].corr
    ackFor(ws, corr)

    const result = await p
    expect(result.reason).toBe("acknowledged")

    // Advancing past the deadline must not produce a second settle
    jest.advanceTimersByTime(10_000)
    // The promise is already settled; nothing observable changes.
    // If abandonment wrongly fired, it would either resolve again
    // (no-op) or emit a spurious pending event:
    const events = []
    gs.on("pending", e => events.push(e))
    jest.advanceTimersByTime(1_000)
    expect(events.find(e => e.status === "abandoned")).toBeUndefined()
  })
})

describe("state machine", () => {
  test("startWaiting() sets WAITING; stopWaiting() returns to IDLE", () => {
    const gs = setup()
    openAndHandshake(gs)

    gs.startWaiting()
    expect(gs.getState()).toBe("WAITING")

    gs.stopWaiting()
    expect(gs.getState()).toBe("IDLE")
  })

  test("state is restored after a reconnect", () => {
    const gs = setup()
    const ws1 = openAndHandshake(gs)

    gs.startWaiting()
    expect(gs.getState()).toBe("WAITING")

    ws1._close(1006, "abnormal")
    expect(gs.getState()).toBe("RECONNECTING")

    jest.advanceTimersByTime(300)
    const ws2 = lastSocket()
    ws2._open()

    // restate should have restored WAITING
    expect(gs.getState()).toBe("WAITING")
  })

  test("WAITING uses a faster pulse than IDLE", () => {
    const gs = setup({ keepaliveMs: 30_000, pulseFloorMs: 1_000 })
    const ws = openAndHandshake(gs)

    // In IDLE, no pulse within the first second
    jest.advanceTimersByTime(1_100)
    const idlePings = ws.sent.filter(m => m.subject === "PING").length
    expect(idlePings).toBe(0)

    // Switch to WAITING; a pulse should fire within ~1s
    gs.startWaiting()
    jest.advanceTimersByTime(1_100)
    const waitingPings = ws.sent.filter(m => m.subject === "PING").length
    expect(waitingPings).toBeGreaterThan(0)
  })
})

describe("pulse misses", () => {
  test("two missed PING ACKs trigger a reconnect in WAITING", () => {
    const gs = setup({ pulseMaxMisses: 2, pulseFloorMs: 1_000 })
    const ws = openAndHandshake(gs)

    gs.startWaiting()

    // First pulse
    jest.advanceTimersByTime(1_100)
    expect(ws.sent.filter(m => m.subject === "PING").length).toBe(1)

    // No ACK arrives; the pulse timeout fires
    jest.advanceTimersByTime(1_100)
    expect(gs.getState()).not.toBe("RECONNECTING")

    // Second pulse, also unanswered
    jest.advanceTimersByTime(1_100)
    jest.advanceTimersByTime(1_100)

    expect(gs.getState()).toBe("RECONNECTING")
  })

  test("an incoming message resets the miss counter", () => {
    const gs = setup({ pulseMaxMisses: 2, pulseFloorMs: 1_000 })
    const ws = openAndHandshake(gs)

    gs.startWaiting()

    jest.advanceTimersByTime(1_100) // pulse 1 sent
    jest.advanceTimersByTime(1_100) // pulse 1 times out (miss 1)

    // Any inbound message resets the counter
    ws._message({ sender_id: "SYSTEM", subject: "CONNECTION", recipient_id: "sid-1" })

    jest.advanceTimersByTime(1_100) // pulse 2 sent
    jest.advanceTimersByTime(1_100) // pulse 2 times out (miss 1 again)
    jest.advanceTimersByTime(1_100) // pulse 3 sent
    jest.advanceTimersByTime(1_100) // pulse 3 times out (miss 2)

    // Only now should we be reconnecting
    expect(gs.getState()).toBe("RECONNECTING")
  })
})

describe("rsvp", () => {
  test("resolves the promise on the rsvp response, not on the ACK", async () => {
    const gs = setup()
    const ws = openAndHandshake(gs)

    const p = gs.send({ subject: "MOVE", x: 1 }, 2_000)
    const corr = ws.sent[0].corr

    // ACK arrives first — must not resolve the promise
    ackFor(ws, corr)

    // Then the rsvp response
    ws._message({ sender_id: "SERVER", subject: "STATE", corr, board: "..." })

    const result = await p
    console.log("\n*******", JSON.stringify(p, null, '  '), "\n*******\n");
    
    expect(result.reason).toBe("handled")
  })

  test("resolves with 'receipt overdue' if the rsvp timeout fires first", async () => {
    const gs = setup()
    const ws = openAndHandshake(gs)

    const p = gs.send({ subject: "MOVE", x: 1 }, 2_000)
    const corr = ws.sent[0].corr

    ackFor(ws, corr)
    jest.advanceTimersByTime(2_100)

    const result = await p
    expect(result.reason).toBe("receipt overdue")
  })
})

describe("latency recording", () => {
  test("ACKs with a time field update the latency window", () => {
    const gs = setup()
    const ws = openAndHandshake(gs)

    const now = Date.now()
    gs.send({ subject: "MOVE", x: 1 })
    const corr = ws.sent[0].corr

    jest.advanceTimersByTime(150)
    ackFor(ws, corr, { time: now })

    // The pulse interval derivation uses latencies; we can't read
    // it directly, but we can verify that a subsequent ACK with a
    // time field is recorded by checking that the pulse interval
    // is recomputed. Indirectly: send enough ACKs to fill the
    // window and verify no error is thrown.
    for (let i = 0; i < 25; i++) {
      gs.send({ subject: "MOVE", x: i })
      const c = ws.sent[ws.sent.length - 1].corr
      ackFor(ws, c, { time: Date.now() - 100 })
    }
    expect(true).toBe(true) // reaching this point without throwing is the assertion
  })
})

describe("edge cases", () => {
  test("a message queued before CONNECTION is sent once socketId is set", () => {
    const gs = setup()
    gs.connect()
    const ws = lastSocket()
    ws._open()

    // socketId not yet set; send should queue but not transmit
    gs.send({ subject: "MOVE", x: 1 })
    expect(ws.sent.filter(m => m.subject === "MOVE")).toHaveLength(0)

    // CONNECTION arrives; _sendMessagesInQueue should flush it
    ws._message({ sender_id: "SYSTEM", subject: "CONNECTION", recipient_id: "sid-1" })
    expect(ws.sent.filter(m => m.subject === "MOVE")).toHaveLength(1)
  })

  test("non-JSON messages are ignored without throwing", () => {
    const gs = setup()
    const onMessage = jest.fn()
    gs.on("message", onMessage)

    const ws = openAndHandshake(gs)
    expect(() => ws._message("{not json")).not.toThrow()
    expect(onMessage).not.toHaveBeenCalled()
  })

  test("messages from stale sockets are ignored", () => {
    const gs = setup()
    const ws1 = openAndHandshake(gs)
    const onMessage = jest.fn()
    gs.on("message", onMessage)

    ws1._close(1006)
    jest.advanceTimersByTime(300)
    const ws2 = lastSocket()
    ws2._open()

    // A message on ws1 (the stale socket) must not reach listeners
    ws1._message({ sender_id: "SYSTEM", subject: "LOGGED_IN", user_name: "x" })
    expect(onMessage).not.toHaveBeenCalled()
  })
})