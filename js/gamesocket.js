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
 * A script can call send() with an object payload and either with
 * or without an rsvp integer timeout delay.
 * 
 * Each outgoing message is identified by a unique corr(elation
 * id). Any response from the server will include the same corr.
 * 
 * The caller will receive a Promise. This will resolve if:
 *   + The message is acknowledged by the server, and no rsvp
 *     timeout delay was given
 *   + The message was handled by the server, which sent a custom
 *     response, using the same corr.
 * 
 * The Promise will reject if the message could not be sent, did
 * not receive any acknowledgement from the server, or did not
 * receive a requested rsvp message after a delay of
 * opts.stopTryingMs (default: 2 minutes).
 * 
 * Callers must therefore be ready to catch rejections. 
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
    // Delay after which to give up trying to send a message
    stopTryingMs:    120000,  

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
      incoming:  new Set(), // emits for incoming non-PING
      pending:   new Set(), // emits "sent" and "acknowledged"
      state:     new Set(),
      reconnect: new Set(), // when _openSocket is called
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
        _schedulePulse(socket)

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
      _emit("close") // missing status
    }


    function _openSocket(reason) {
      _teardown(reason, 1000)

      const myGen = ++generation
      const ws    = new WebSocket(opts.url + opts.path)

      ws._gen        = myGen
      // Timeout values
      ws._pulseTimer = null // timeout to send next ping
      // Number of pulse messages that did not get acknowledged
      ws._ackMiss    = 0
      // Promises waiting for ACK or rsvp
      // { corr => { resolve, timer, reject }, ... }
      ws._pending    = new Map() // for ACK messages
      ws._rsvp       = new Map() // for rsvp messages

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
      clearTimeout(ws._pulseTimer)
      ws._pulseTimer = null

      // ws.pending is { corr => { resolve, timer, reject }, ... }
      for (const { timer } of ws._pending.values()) {
        clearTimeout(timer)
      }
      for (const { timer } of ws._rsvp.values()) {
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
      _schedulePulse(ws)
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

      const status = {
        generation: ws._gen,
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
      }
      _emit("close", status)

      opts.c.showStatus(isConnected())

      if (closedByUser) {
        _setState("IDLE")
        return
      }
      _setState("RECONNECTING")
    }


    /**
     * @param {socket} ws
     * @param {string} message should be a JSON string with a
     *                 { ..., data, ... } field.
     *
     * There are four types of incoming messages:
     * 0. Messages with sender_id "SYSTEM", such as "CONNECTION",
     *    which is sent when a socket opens on the server, and
     *    "LOGGED_IN", which is sent in response to a "LOG_IN"
     *    request. Such messages will trigger:
     *
     *    _emit("incoming", message)
     *
     * 1. ACK messages to acknowledge that the server received
     *    an outgoing message.
     *    For an outgoing "PING", this will be the only response.
     *    For user-initiated outgoing messages, this will trigger…
     *
     *    _emit("pending", {corr, message, status: "acknowledged"})
     *
     *    … where `message` is the original outgoing message.
     *
     * 2. A custom response to an outgoing message, for which an
     *    rsvp timeout has been set. If the response is received
     *    before the timeout triggers, this will trigger…
     *
     *    _emit("pending", {corr, message, status: "handled"})
     *
     *    … where `message` is the new incoming response.
     *
     *    If the timeout triggered, then there is no longer any
     *    record that an rsvp was requested. Such a late response
     *    will be treated like a third party message (3) below.
     *
     * 3. An unsolicited message from the server or a third party.
     *    Such a message may include a `corr` value, but this will
     *    not correspond to any `corr` values created by this
     *    client. Such a message will trigger:
     *
     *    _emit("incoming", message)
     */
    function _onMessage(ws, { data }) {
      if (ws !== socket) { return }

      let message
      try { message = JSON.parse(data) } catch { return }

      // Any inbound message proves the socket is alive
      ws._ackMiss = 0

      // Wait a while before sending a new pulse message
      _schedulePulse(ws)

      // Handle ACK and private SYSTEM messages only internally
      if (message.subject === "ACK" && message.corr) {
        return _settleAck(ws, message)

      } else if (message.sender_id === "SYSTEM") {
        // CONNECTION, LOGGED_IN, ...?
        return _treatSystemMessage(message)
        // calls _emit("incoming", message)
      }

      // Check if the incoming message is the response to an
      // outgoing request from this client, with an rsvp
      const receipt = ws._rsvp.get(message.corr)

      if (receipt) {
        _treatResponse(ws, message, receipt)

      } else {
        // Neither ACK nor response nor unshared SYSTEM; possibly
        // a message from a third party.
        _emit("incoming", message)
      }
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
          socketId = message.recipient_id // private and temporary
          _emit("incoming", message)
          // The socket is now ready to send outgoing messages
          _sendMessagesInQueue()
        break // status available through isConnected()

        case "LOGGED_IN":
          // Store user_id and user_name locally. These can be
          // accessed through gameSocket.getUser()
          ({ user_id, user_name } = message)
          opts.c.log("LOGGED_IN", message.corr.slice(0, 8))
          _emit("incoming", {
            subject: message.subject,
            user_id,
            user_name
          })
      }
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

        socket._pulseTimer = null
        socket._ackMiss  = 0
        _schedulePulse(socket)
      }

      if (next === "RECONNECTING") {
        // The connection was dropped and should soon reopen
        _scheduleReconnect()
      }

      _emit("state", { from: prev, to: next })
    }


    // KEEPALIVE / IDLE (slow) / WAITING (fast)

    /**
     * Sent by _onOpen cpr _setState _onMessage _scheduleReconnect
     * @param {socket} ws
     * @returns
     */
    function _schedulePulse(ws) {
      if (ws !== socket) { return }

      clearTimeout(ws._pulseTimer)

      const pulse = () => send({ subject: "PING" })
      const delay = _pulseInterval()
      ws._pulseTimer = setTimeout(pulse, delay)
    }


    function _getAckTimeout() {
      if (latencies.length < 3) {
        return opts.ackTimeoutMs
      }

      const sorted = [...latencies].sort((a, b) => a - b)
      const p95    = sorted[Math.floor(sorted.length * 0.95)]

      return p95 * opts.pulseRTTFactor
    }


    function _pulseInterval () {
      if (state === "IDLE") {
        return opts.keepaliveMs // slow
      }
      // When WAITING, check pulse much more frequently, depending
      // on the expected Round Trip Time

      ackTimeoutMs = _getAckTimeout()
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
      clearTimeout(reconnectTmr)
      reconnectTmr = null
    }


    // SENDING //

    /**
     *
     * @param {object} payload must be an object
     * @param {mixed} rsvp may be an integer, which should be
     *        bigger than ackTimeoutMs. 
     */
    function send(payload, rsvp) {
      // opts.c.log("send", payload)
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
          corr,
          resolve,
          reject,
          rsvp,
          retries: 0
        }

        if (message.subject !== "PING") {
          // This is a user action message. Prepare to send it
          // multiple times.
          queue.set(corr, envelope)
          _emit("pending", { corr, message, status: "queued" })
        }

        if (!socketId
         || !socket
         || socket.readyState !== WebSocket.OPEN
        ) {
          if (message.subject === "PING") {
            // Drop the PING; it should never be queued
            resolve("socket not ready")
          }
          return // wait for the next socket to open
        }

        _sendQueuedMessage(socket, envelope)
      })
    }


    function _sendQueuedMessage(ws, envelope) {
      if (!socketId || ws.readyState !== WebSocket.OPEN) {
        // This call will be made again when a connection is ready
        return
      }

      const { message, corr, rsvp } = envelope
      // Update message boilerplate
      message.sender_id = socketId // won't change after first time
      message.time = Date.now() // for latency

      if (!message.abandonAfter) {
        // Prepare to reject the sending of the message if 
        message.abandonAfter = Date.now() + opts.stopTryingMs
      }

      // Create a timeout for the ACK message. A simple PING will
      // timeout differently from a user-initiated message.
      const timedOut = () => {
        message.subject === "PING"
          ? _treatMissedACK(ws, corr)
          : _retrySend(ws, corr)
      }
      envelope.timer = setTimeout(timedOut, ackTimeoutMs)

      // Store details of the message to be sent in the ws instance
      ws._pending.set(corr, envelope)

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
      // this custom response if rsvp > 0. Repurpose the existing
      // resolve() callback for the response that shows that the
      // server has handled the request.
      if (rsvp) {
        _requestReceipt(ws, envelope)
      }

      if (message.subject !== "PING") {
        // Indicate message has been sent, but not yet ACK'd
        _emit("pending", { corr, message, status: "sent" })
      }
    }


    function _requestReceipt(ws, envelope) {
      const { corr, rsvp } = envelope

      // Add a receipt timeout to the sending socket
      const timedOut = () => _treatMissedReceipt(ws, corr)
      const timer = setTimeout(timedOut, rsvp)
      const receipt = { corr, timer }

      ws._rsvp.set(corr, receipt)
    }


    /**
     * Triggered by the envelope.timer after ackTimeoutMs, if no
     * ACK message was received by then from the server
     * @param {socket} ws 
     * @param {string} corr is the unique transaction id
     */
    function _retrySend(ws, corr) {
      if (state === "RECONNECTING") {
        // _sendMessagesInQueue will resend when a socket reopens
        return
      }

      opts.c.log("retrySend", corr.slice(0, 8))
      // The timer stored for this corr in ws._rsvp has triggered.
      // Delete the memory of it. A new rsvp will be created if
      // the message gets resent
      ws._rsvp.delete(corr)

      // Give up trying to send this message if too much time has
      // passed
      const envelope = queue.get(corr)
      if (Date.now() > envelope.abandonAfter) {
        return _abandonSend(ws, corr)
      }

      if (!envelope.retried) {
        // The first ACK message timed out. Try again immediately
        // with the same socket, just in case this was a glitch.
        clearTimeout(envelope.timer)
        _sendQueuedMessage(ws, envelope)
        envelope.retried = 1

      } else {
        // The immediate second send also timed out. Schedule a
        // reconnection, which will resend the entire contents of
        // the queue when the connection is re-established
        envelope.retried = false // so next socket will resend
        return _setState("RECONNECTING")
      }
    }


    function _abandonSend(ws, corr) {
      const envelope = queue.get(corr)
      if (!envelope) { return }

      // The _pending timer already triggered. The envelope will
      // not be needed if the message is not sent again.
      ws?._pending.delete(corr)
      queue.delete(corr)

      const { timer, message, resolve } = envelope
      clearTimeout(timer)
      resolve({ reason: "abandoned", message: message })
      _emit("pending", { corr, message, status: "abandoned" })
    }


    /**
     * Called by _onMessage() for messages that requested an rsvp
     * from the server
     * @param {*} ws 
     * @param {*} message 
     * @param {object} receipt will be { timer }
     */
    function _treatResponse(ws, message, receipt) {
      // Don't let the receipt timeout trigger
      const { corr, timer } = receipt
      clearTimeout(timer)
      ws._rsvp.delete(corr)

      // Resolve the Promise created by send() now that the
      // outgoing message has been acted on.
      const envelope = ws._pending.get(corr)
      ws._pending.delete(corr)
      envelope.resolve({ reason: "rsvp received" })

      // The calling script can use corr to associate this response
      // with the outgoing message whose progress it was informed
      // about in previous "pending" updates.
      _emit("pending", { corr, message, status: "handled" })
    }


    /**
     * An overdue receipt is not proof that the server did not act
     * on the outgoing message. The server-side action may have
     * succeeded but the response packet was delayed or lost. No
     * new information is available about the state on the server.
     *
     * @param {socket} ws is the socket the message was sent by
     * @param {string} corr identifies the message that requested
     *                 an rsvp
     */
    function _treatMissedReceipt(ws, corr) {
      const { message, reject } = ws._rsvp.get(corr)
      ws._rsvp.delete(corr) // so a very late message is ignored
      reject({ reason: "receipt overdue" })

      _emit("pending", { corr, message, status: "unknown" })
    }


    /**
     * Triggered by the envelope.timer stored in
     * ws._pending.get(corr) after ackTimeoutMs
     * @param {socket} ws is the socket the message was sent by
     * @param {string} corr identifies the outgoing ACK message
  
     * @returns 
     */
    function _treatMissedACK(ws, corr) {
      opts.c.log("TREAT MISSED ACK", corr.slice(0, 8))

      // Nothing is awaitng the Promise created by send() for
      // this ACK message, so simply resolve it silently
      const { resolve, message } = ws._pending.get(corr)
      ws._pending.delete(corr) // so a very late message is ignored
      resolve({ reason: "missed ACK", message })

      if (ws !== socket) { // A new socket is already active
        return
      }

      if (ws._ackMiss >= opts.pulseMaxMisses) {
        ws.close(1000, "reconnecting")
      }

      _schedulePulse(ws)

      ws._ackMiss++
    }


    function _settleAck (ws, message) { // incoming message
      const envelope = ws._pending.get(message.corr)
      if (!envelope) { return }

      // If message had been queued, remove it from the queue
      // now that it has been delivered...
      const { corr, time } = message // subjectalways "ACK"
      queue.delete(corr)
      // ... and is no longer pending acknowledgement
      clearTimeout(envelope.timer)

      if (!ws._rsvp.get(corr)) {
        // ACK resolves the Promise created by send() and no rsvp
        // is expected
        envelope.resolve({ reason: "acknowledged", message })
        ws._pending.delete(corr)
      } // else rsvp will resolve with "handled" or "unknown"

      if (envelope.message.subject !== "PING") {
        _emit("pending", {
          corr,
          message: envelope.message,
          status: "acknowledged"
        })
      }

      _recordLatency(time, corr) // corr is just for debugging
    }


    function _recordLatency (time, corr) { // corr for debugging
      if (typeof time !== "number") {
        // This should never happen
        return
      }

      const ms = Date.now() - time

      if (!Number.isFinite(ms) || ms < 0) { return }

      latencies.push(ms)
      if (latencies.length > opts.latencyWindow) {
        latencies.shift()
      }

      opts.c.log("ACK", { ms, corr: corr.slice(0, 8)})
    }


    /**
     * Called by _onOpen. Iterates through the messages in queue
     * to resend them
     */
    function _sendMessagesInQueue() {
      const corrs = []
      queue.forEach( a => corrs.push(a) )
      opts.c.log("opts.c _sendMessagesInQueue", corrs)

      _identifyUser()

      const now = Date.now()

      for (const [corr, envelope] of [...queue]) {
        if (now > envelope.abandonAfter) {
          _abandonSend(socket, corr)
          continue
        }

        _sendQueuedMessage(socket, envelope)
      }
    }
  }


  // ── Export ──────────────────────────────────────────────────────
  if (typeof module !== "undefined" && module.exports) {
    module.exports = { createGameSocket }
  } else {
    root.GameSocket = { createGameSocket }
  }
})(typeof globalThis !== "undefined" ? globalThis : this)