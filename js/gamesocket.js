/**
 * frontend/js/websocket.js
 *
 * A WebSocket client designed for turn-based games. While waiting
 * for a turn, a player may not be sending any messages to the
 * server, but will be expecting the WebSocket connection to be
 * alive so that the server can forward messages from other
 * players.
 *
 * DESCRIPTION
 * -----------
 * On an unstable network the WebSocket connection can break often.
 * While the player is waiting for a turn, the app needs to be
 * constantly checking if the connection is still able to deliver
 * incoming messages. When the player is actively playing, the app
 * needs to ensure that a broken connection is restored as quickly
 * as possible after an outgoing message fails, and then resend the
 * message.
 *
 * If the server detects a dropped connection, it cannot reconnect,
 * but it can send a message to other connected users, to warn them
 * of the situation.
 *
 * Restoring connectivity means checking regularly if messages to
 * the server are being acknowledged. The frequency of these
 * messages depends on the user's current state:
 *
 * + IDLE (not actively involved in the game)
 * + WAITING incoming messages
 * + ACTIVE (sending outgoing messages)
 *
 * This WebSocket client will:
 *
 *  + Send a heartbeat message every 25-30 seconds in IDLE mode
 *    to ensure the connection remains open
 *  + Send a heartbeat every second or so in WAITING mode
 *  + Request an ACK(nowledgement) of all outgoing messages, and
 *    will reconnect and resend any messages that do not receive
 *    an ACK response within a reasonable time.
 *  + A user-initiated message that failed to receive an ACK will
 *    be resent with the same corr(elation id), so that the server
 *    can ignore it if it was already treated (idempotency).
 *
 * The heartbeat delay will be calculated dynamically based on the
 * longest response time over the past few cycles, and the current
 * state (IDLE or WAITING).
 *
 * ////////////////////////////////////////////////////////////// *
 * A separate WebSocket server script handles these features:
 *
 *  + Detecting when a client connection breaks, and warning other
 *    connected users
 *  + Maintaining a centralised game state that is updated by all
 *    incoming player actions
 *  + Checking the corr value of any incoming message, and ignoring
 *    duplicates after an initial treatment
 *  + Resending the entire game state each time a new connection
 *    is made
 * ////////////////////////////////////////////////////////////// *
 *
 *
 */


;(function (root){
  "use strict"

  const DEFAULTS = {
    path:            "/ws",
    // Heartbeat rates and RTT tolerance
    keepaliveMs:     30000, // when little traffic is expected
    pulseFloorMs:    1000,  // every second when expecting incoming
    pulseCeilMs:     15000, // allows 5000ms RTT on slow connection
    pulseRTTFactor:  3,
    pulseMaxMisses:  2,
    latencyWindow:   20,    // max length of latencies array
    ackTimeoutMs:    1000,  // adjusted depending on latencies
    // Staggering reconnection attempts
    reconnectBaseMs: 250,   // 500, 1000, 2000, 4000, 8000, 16000,
    reconnectMaxMs:  32000, // after 8 attempts or 63.75 seconds

    // Console
    c: {
      addToList:  () => {},
      log:        () => {},
      logAck:     () => {},
      logCall:    () => {},
      showStatus: () => {},
      statistics: () => {},
    }
  }


  function createGameSocket(userOptions) {
    const opts = Object.assign({}, DEFAULTS, userOptions)
    if (!opts.url) {
      throw new Error("GameSocket: url is required")
    }


    // INTERNAL STATE //

    let socket       = null
    let generation   = 0
    let socketId     = ""
    let state        = "IDLE" // IDLE | WAITING | RECONNECTING
    let ackTimeoutMs = opts.ackTimeoutMs
    let reconnectMs  = opts.reconnectBaseMs // pause till reconnect
    let reconnectTmr = null
    let closedByUser = false

    // Unique values provided by server on login
    let user_id      = ""
    let user_name    = ""

    const latencies  = []
    const listeners  = {
      open:      new Set(),
      close:     new Set(),
      error:     new Set(),
      message:   new Set(), // emits for incoming non-PING
      pending:   new Set(), // emits "sent" and "acknowledged"
      state:     new Set(),
      reconnect: new Set(),
      retrying:  new Set(), // each time _scheduleReconnect called
    }


    // PUBLIC API //
    const api = {
      cpr,
      connect,
      disconnect,
      isConnected,
      // Messages
      send,
      // State
      startWaiting,
      stopWaiting,
      getState: () => state,
      // Logged in user
      getUser: () => ({ user_name, user_id }),
      // Listeners
      on,
      off,
    }
    return api



    // EVENT EMITTERS //

    function on(event, fn) {
      if (!listeners[event]) {
        // open
        // close
        // error
        // message
        // pending
        // state
        // reconnect
        // retrying
        throw new Error(`unknown event: ${event}`)
      }

      listeners[event].add(fn)

      return () => {
        listeners[event].delete(fn)
      }
    }


    function off(event, fn) {
      listeners[event]?.delete(fn)
    }


    function _emit(event, payload) {
      for (const fn of listeners[event]) {
        try {
          fn(payload)

        } catch (error) {
          console.error(error)
        }
      }
    }


    // LIFECYCLE //

    function cpr() {
      // If there is an active socket, sends a manual PING, which
      // will call _scheduleReconnect() immediately if it fails to
      // be acknowledged. If there is no active socket, requests
      // a reconnection.
      if (socket) {
        _schedulePulse(socket, true)

      } else {
        _setState("RECONNECTING") // ignored if RECONNECTING now
      }
    }


    function connect() {
      closedByUser = false
      _openSocket("connect()")
    }


    function disconnect() {
      closedByUser = true
      _teardown("user disconnect", 1000)
      _cancelReconnect()
      _setState("IDLE") // "DOWN"
    }


    function _openSocket(reason) {
      _teardown(reason, 1000)

      const myGen = ++generation
      const ws    = new WebSocket(opts.url + opts.path)

      ws._gen        = myGen
      // Timeout values
      ws._ackTimer   = null // timeout to trigger on missed ACK
      ws._pulseTimer = null // timeout to send next ping
      // Number of pulse messages that did not get acknowledged
      ws._ackMiss    = 0
      // Promises waiting for ACK
      // { corr => { resolve, timer, reject }, ... }
      ws._pending    = new Map()

      ws.onopen      = (e) => _onOpen(ws, e)
      ws.onerror     = (e) => _onError(ws, e)
      ws.onclose     = (e) => _onClose(ws, e)
      ws.onmessage   = (e) => _onMessage(ws, e)

      // Adopt this ws as the currently active socket
      socket = ws
      _emit("reconnect", { generation: myGen, reason })

      opts.c.showStatus(isConnected())
    }


    function _teardown(reason, code = 1000, text = reason) {
      if (!socket) { return }

      const ws = socket
      socket = null

      _clearTimers(ws)

      const active = ws.readyState === WebSocket.OPEN
                  || ws.readyState === WebSocket.CONNECTING
      if (active) {
        try {
          ws.close(code, text)
        } catch { /* already closing */ }
      }

      opts.c.showStatus(isConnected()) // false: socket = null
    }


    function _clearTimers(ws) {
      clearTimeout(ws._ackTimer)
      clearTimeout(ws._pulseTimer)
      ws._ackTimer = ws._pulseTimer = null

      // ws.pending is { corr => { resolve, timer, reject }, ... }
      for (const { timer } of ws._pending.values()) {
        clearTimeout(timer)
      }

      ws._pending.clear()
    }


    // SOCKET EVENTS //

    function _onOpen (ws) {
      if (ws !== socket) { return }

      socketId = ""
      reconnectMs = opts.reconnectBaseMs

      _setState("IDLE")
      _schedulePulse(ws, "_onOpen")
      _emit("open", { generation: ws._gen })

      opts.c.showStatus(isConnected())
    }


    function _onError(ws) {
      if (ws !== socket) return
      _emit("error", { generation: ws._gen })
    }


    function _onClose (ws, event) {
      if (ws !== socket) {
        return opts.c.showStatus(isConnected()) // false
      }

      _clearTimers(ws)
      socket = null
      socketId = ""

      _emit("close", {
        generation: ws._gen,
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
      })

      opts.c.showStatus(isConnected())

      if (closedByUser) {
        _setState("IDLE")
        return
      }
      _setState("RECONNECTING")
    }

    function _onMessage(ws, { data }) {
      if (ws !== socket) { return }

      let message
      try { message = JSON.parse(data) } catch { return }

      // Any inbound message proves liveness
      ws._ackMiss = 0
      clearTimeout(ws._pulseTimer)
      ws._pulseTimer = null
      // Wait a while before sending a new keepalive message
      _schedulePulse(ws, "_onMessage")

      // Handle ACK and private SYSTEM messages only internally
      if (message.subject === "ACK" && message.corr) {
        return _settleAck(ws, message)

      } else if (message.sender_id === "SYSTEM") {
        // CONNECTION, LOGGED_IN, ...?
        const share = _treatSystemMessage(message)
        if (!share) {
          return
        }
      }

      // Neither ACK nor unshared SYSTEM
      _emit("message", message)

      opts.c.log("incoming", message)
    }


    function _treatSystemMessage(message) {
      switch (message.subject) {
        case "CONNECTION":
          socketId = message.recipient_id
          opts.c.log("socketId set to", `${socketId.slice(0, 8)}…`)
          break // status available through isConnected()

        case "LOGGED_IN":
          ({ user_id, user_name } = message)

          opts.c.log(`user_name: ${user_name}${user_id ? ", user_id: "+user_id : ""}`)
          return true // app listeners may want to know
      }

      return false
    }


    function _settleAck (ws, message) {
      const pending = ws._pending.get(message.corr)
      if (!pending) { return }

      ws._pending.delete(message.corr)
      clearTimeout(pending.timer)

      if (typeof message.time === "number") {
        _recordLatency(message.time)
      }

      pending.resolve(message)

      _emit("pending", { message, acknowledged: true })
    }


    function _recordLatency (time) {
      const ms = Date.now() - time

      if (!Number.isFinite(ms) || ms < 0) { return }

      latencies.push(ms)
      if (latencies.length > opts.latencyWindow) {
        latencies.shift()
      }

      opts.c.log("ACK", ms)
    }


    // STATE MACHINE //

    function isConnected () {
      return !!socket && socket.readyState === WebSocket.OPEN
    }


    function _setState (next) {
      if (state === next) { return }

      const prev = state
      state = next

      if (socket && (next === "IDLE" || next === "WAITING")) {
        clearTimeout(socket._pulseTimer)
        socket._pulseTimer = null
        socket._ackMiss  = 0
        _schedulePulse(socket, "_setState")
      }

      if (next === "RECONNECTING") {
        // The connection was dropped and should soon reopen
        _scheduleReconnect()
      }

      _emit("state", { from: prev, to: next })

      opts.c.log(`_setState: ${next}`, "#now")
    }


    /**
     * An external script requested a higher frequency of checks
     * so the client can be sure of receiving incoming messages
     */
    function startWaiting() {
      if (state === "RECONNECTING") { return }

      _setState("WAITING")
    }


    /**
     * An external script considers that incoming messages are not
     * a high priority for now. Outgoing messages will serve to
     * check that the connection is still alive.
     */
    function stopWaiting() {
      if (state === "WAITING") {
        _setState("IDLE")
      }
    }


    // KEEPALIVE / IDLE (slow) / PULSE (fast if WAITING)

    /**
     * Sent by _onOpen cpr _setState _onMessage _scheduleReconnect
     * @param {socket} ws
     * @returns
     */
    function _schedulePulse(ws, from) {
      if (ws !== socket) { return }

      clearTimeout(ws._pulseTimer)

      const confirmPulse = () => {
        if (ws !== socket) {
          // A new socket is already active
          return
        }

        ws._pulseTimer = null

        if (ws._ackMiss >= opts.pulseMaxMisses) {
          _setState("RECONNECTING")
          return
        }

        ws._ackMiss++
        send({ subject: "PING" })
        // opts.c.log(`confirmPulse timeout (${state})`, "#now")

        // _schedulePulse(ws, "confirmPulse")
      }

      const delay = _pulseInterval("_schedulePulse")
      ws._pulseTimer = setTimeout(confirmPulse, delay)

      // opts.c.log(`_schedulePulse (delay: ${delay})`, "#now")
    }


    function _setAckTimeout(from) {
      if (latencies.length < 3) {
        return opts.ackTimeoutMs
      }

      const sorted = [...latencies].sort((a, b) => a - b)
      const p95    = sorted[Math.floor(sorted.length * 0.95)]
      ackTimeoutMs = p95 * opts.pulseRTTFactor

      opts.c.log(`ackTimeoutMs`, ackTimeoutMs)
    }


    function _pulseInterval (from) {
      if (state === "IDLE") {
        return opts.keepaliveMs // slow
      }

      // opts.c.log(`_pulseInterval`)
      // When WAITING, check pulse much more frequently, depending
      // on the expected Round Trip Time

      _setAckTimeout("_pulseInterval")
      return Math.min(opts.pulseCeilMs,
             Math.max(opts.pulseFloorMs, ackTimeoutMs))
    }


    // RECONNECT //

    /**
     * Sent when _setState("RECONNECTING") is called, which occurs
     * in _onClose() and _schedulePulse after an ACK response is
     * missed. If the _openSocket() attempt fails, _onClose() will
     * be called again, but reconnectMs will have increased, to
     * give the server progressively more time to react.
     */
    function _scheduleReconnect () {
      _cancelReconnect()

      reconnectTmr = setTimeout(() => {
        reconnectTmr = null
        _openSocket("reconnect timer")
      }, reconnectMs)

      // Wait longer before next reconnect attempt
      reconnectMs = Math.min(opts.reconnectMaxMs, reconnectMs * 2)

      _emit("retrying", { reconnectMs} )
    }


    function _cancelReconnect() {
      clearTimeout(reconnectTmr)
      reconnectTmr = null
    }


    // SENDING //

    function send(payload) {
      return new Promise((resolve, reject) => {
        if (!socketId
         || !socket
         || socket.readyState !== WebSocket.OPEN
        ) {
          reject(new Error("socket not ready"))
          return
        }

        const corr  = crypto.randomUUID()
        const timer = setTimeout(() => {
          socket._pending.delete(corr)
          reject(new Error("ACK timeout"))
        }, ackTimeoutMs)

        socket._pending.set(corr, { resolve, timer, reject })
        const message = {
          ...payload,
          sender_id: socketId,
          corr,
          time: Date.now(),
        }

        socket.send(JSON.stringify(message))

        if (message.subject !== "PING") {
          _emit("pending", { message, acknowledged: false })
          opts.c.log("outgoing", message)
        }
      })
    }
  }



  // ── Export ──────────────────────────────────────────────────────
  if (typeof module !== "undefined" && module.exports) {
    module.exports = { createGameSocket }
  } else {
    root.GameSocket = { createGameSocket }
  }
})(typeof globalThis !== "undefined" ? globalThis : this)