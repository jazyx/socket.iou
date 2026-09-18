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
    let restate      = state // for after a reconnection
    let ackTimeoutMs = opts.ackTimeoutMs
    let reconnectMs  = opts.reconnectBaseMs // pause till reconnect
    let reconnectTmr = null
    let closedByUser = false

    // Unique values provided by server on login
    let user_id      = "" // unique _id from User database
    let user_name    = "" // human-readable name (not unique)

    const queue      = new Map() // for messages to resend
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
      _setState("IDLE")
    }


    function _openSocket(reason) {
      opts.c.log("_openSocket", reason)
      _teardown(reason, 1000)

      const myGen = ++generation
      const ws    = new WebSocket(opts.url + opts.path)

      ws._gen        = myGen
      // Timeout values
      ws._ackTimer   = null // timeout to trigger on missed ACK
      ws._pulseTimer = null // timeout to send next ping
      // Number of pulse messages that did not get acknowledged
      ws._ackMiss    = 0
      // Promises waiting for ACK or rsvp
      // { corr => { resolve, timer, reject }, ... }
      ws._pending    = new Map() // for ACK messages
      ws._expected   = new Map() // for rsvp messages

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
      opts.c.log(`clearTimers`)

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

      _setState(restate)
      _schedulePulse(ws, "_onOpen")
      _emit("open", { generation: ws._gen })

      _sendMessagesInQueue()
      opts.c.showStatus(isConnected())
      opts.c.log("_onOpen", {state, restate})

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

      const status = {
        generation: ws._gen,
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
      }
      _emit("close", status)

      opts.c.showStatus(isConnected())
      opts.c.log("_onClose", status)
      opts.c.log("_onClose", { closedByUser, state })

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
      clearTimeout(ws._ackTimer)
      // opts.c.log(`clearTimeout ws._ackTimer ${ws._ackTimer}`)
      ws._ackTimer = null

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


    // HOUSEKEEPING // HOUSEKEEPING // HOUSEKEEPING //

    function _identifyUser() {
      if (user_name || user_id) {
        send({
          recipient_id: "SYSTEM",
          subject: "LOG_IN",
          user_id,
          user_name
        })
      }
    }


    function _treatSystemMessage(message) {
      switch (message.subject) {
        case "CONNECTION":
          socketId = message.recipient_id
          opts.c.log("socketId set to", `${socketId.slice(0, 8)}…`)
          _identifyUser()
          break // status available through isConnected()

        case "LOGGED_IN":
          ({ user_id, user_name } = message)

          opts.c.log(`user_name: ${user_name}${user_id ? ", user_id: "+user_id : ""}`)
          return true // app listeners may want to know
      }

      return false
    }


    // STATE MACHINE //

    function isConnected () {
      return !!socket && socket.readyState === WebSocket.OPEN
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


    function _setState (next) {
      if (state === next) { return }

      const prev = state
      state = next
      if (state !== "RECONNECTING") {
        restate = state
      }

      if (socket && (next === "IDLE" || next === "WAITING")) {
        clearTimeout(socket._pulseTimer)
        opts.c.log(`_SET_STATE:clearTimout socket._pulseTimer ${socket._pulseTimer}`)

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


    // KEEPALIVE / IDLE (slow) / PULSE (fast if WAITING)

    /**
     * Sent by _onOpen cpr _setState _onMessage _scheduleReconnect
     * @param {socket} ws
     * @returns
     */
    function _schedulePulse(ws, from) {
      if (ws !== socket) { return }

      clearTimeout(ws._pulseTimer)
      // opts.c.log(`clearTimeout ws._pulseTimer ${ws._pulseTimer}`)


      const pulse = () => send({ subject: "PING" })
      const delay = _pulseInterval("_schedulePulse")
      ws._pulseTimer = setTimeout(pulse, delay)
      opts.c.log(`setTimeout ws._pulseTimer ${ws._pulseTimer} ${delay} ${pulse}`)

      // opts.c.log(`_schedulePulse (delay: ${delay})`, "#now")
    }


    function _setAckTimeout(from) {
      if (latencies.length < 3) {
        return opts.ackTimeoutMs
      }

      const sorted = [...latencies].sort((a, b) => a - b)
      const p95    = sorted[Math.floor(sorted.length * 0.95)]
      ackTimeoutMs = p95 * opts.pulseRTTFactor

      // opts.c.log(`ackTimeoutMs`, ackTimeoutMs)
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
      opts.c.log("_scheduleReconnect()")
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
      // opts.c.log(`clearTimeout reconnectTmr ${reconnectTmr}`)
      clearTimeout(reconnectTmr)
      reconnectTmr = null
    }


    // SENDING //

    /**
     * 
     * @param {object} payload must be an object
     * @param {mixed} rsvp ma
     * @returns 
     */
    function send(payload, rsvp) {
      opts.c.log("send", payload)
      if (typeof payload !== "object") {
        throw new Error("Object required as payload for send")
      }
      rsvp = parseInt(rsvp) || 0
      

      return new Promise((resolve, reject) => {
        // Prepare message for sending, and queue it, regardless
        // of whether the socket is able to send it yet
        const corr  = crypto.randomUUID()
        const message = {
          ...payload,
          corr
        }

        const envelope = {
          message,
          resolve,
          reject,
          rsvp,
          retries: 0
        }

        if (message.subject !== "PING") {
          // This is a user action message. Prepare to send it
          // multiple times.
          queue.set(corr, envelope)
          _emit("pending", { message, status: "queued" })
        }

        if (!socketId
         || !socket
         || socket.readyState !== WebSocket.OPEN
        ) {
          return // wait for the next socket to open
        }

        _sendQueuedMessage(socket, envelope)
      })
    }


    function _sendQueuedMessage(ws, envelope) {
      if (!socketId) {
        // This call will be made again when a connection is ready
        return
      }

      const { message, resolve, reject, rsvp } = envelope
      // Update message boilerplate
      message.sender_id = socketId // won't change after first time
      message.time = Date.now()

      // Create a timeout for the ACK message. A simple PING will
      // timeout differently from a user-initiated message.
      const timedOut = () => {
        message.subject === "PING"
          ? _treatMissedACK(ws, message.corr)
          : _retrySend(ws, message.corr)
      }
      const timer = setTimeout(timedOut, ackTimeoutMs)

      // Store details of the sent message in the ws instance
      ws._pending.set(message.corr, { resolve, timer, reject })

      ws.send(JSON.stringify(message))
      // One of two things will now happen:
      // 1. An incoming ACK message with corr will indicate that
      //    the connection was still alive and the server received
      //    the message
      // 2. The timeout will trigger, suggesting a connection
      //    issue.
      //    a) For ACK messages, this can happen up to 
      //       pulseMaxMisses times before a new socket connection
      //       is scheduled. ACK messages will never be resent.
      //    b) For user-initiated messages, a first missed ACK will
      //       force the message to be resent with the same
      //       socket. A second missed ACK will reschedule a new
      //       connection, and the message will be resent with the
      //       new socket.

      // A user-initiated message may also expect a custom
      // response. This will be identified by the same corr, but
      // may have any subject and payload. Generate a timeout for
      // this custem response if rsvp > 0.
      if (rsvp) {
        const timer = setTimeout(_treatMissedResponse, rsvp)
        ws._expected.set(message.corr, { resolve, timer, reject })
      }

      if (message.subject !== "PING") {
        // Indicate message has been sent, but not yet ACK'd
        _emit("pending", { message, status: "sent" })
      }
    }


    function _treatMissedACK(ws, corr) {
      opts.c.log("TREAT MISSED ACK",{
        miss: ws._ackMiss,
        socket: socket._gen,
        ws: ws._gen,    
      })

      // Resolve the send() Promise silently 
      const { resolve, message } = ws._pending.get(corr)
      ws._pending.delete(corr) // so a very late message is ignored
      resolve({ reason: "missed ACK", message })

      if (ws !== socket) { // A new socket is already active
        return
      }

      _schedulePulse(ws, "_treatMissedACK")

      if (ws._ackMiss >= opts.pulseMaxMisses) {
        ws.close(4000, "reconnecting")
        return _setState("RECONNECTING")
      }

      ws._ackMiss++
    }


    function _treatMissedResponse(param) {
      // function body
    }


    function _retrySend(ws, corr) {
      if (state === "RECONNECTING") { // resend when socket opens
        return
      }

      const envelope = queue.get(corr)
      if (!envelope.retried) {
        // The first ACK message timed out. Try again immediately
        // with the same socket, just in case this was a glitch.
        _sendQueuedMessage(ws, envelope)
        envelope.retried = true

      } else {
        // The immediate second send also timed out. Schedule a
        // reconnection, which will re send the entire contents of
        // the queue when the connection is re-established
        envelope.retried = false // so next socket will resend
        return _setState("RECONNECTING")
      }
    }


    /**
     * Called by _onOpen. Iterates through the messages in queue
     * to resend them
     */
    function _sendMessagesInQueue() {
      for (const envelope of queue.values()) {
        _sendQueuedMessage(socket, envelope)
      }
    }


    function _settleAck (ws, message) {
      const pending = ws._pending.get(message.corr)
      if (!pending) { return }

      // If message had been queued, remove it from the queue
      // now that it has been delivered...
      queue.delete(message.corr)
      // ... and is no longer pending acknowledgement
      ws._pending.delete(message.corr)
      clearTimeout(pending.timer)

      // opts.c.log(`clearTimeout pending.timer ${pending.timer}`)

      if (typeof message.time === "number") {
        _recordLatency(message.time)
      }

      pending.resolve(message)

      _emit("pending", { message, status: "acknowledged" })
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
  }



  // ── Export ──────────────────────────────────────────────────────
  if (typeof module !== "undefined" && module.exports) {
    module.exports = { createGameSocket }
  } else {
    root.GameSocket = { createGameSocket }
  }
})(typeof globalThis !== "undefined" ? globalThis : this)