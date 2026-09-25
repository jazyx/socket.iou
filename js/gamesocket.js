/**
 * frontend/js/websocket.js
 *
 * A WebSocket client designed for turn-based games. While waiting
 * for a turn, a player may not be sending any messages to the
 * backend, but will be expecting the WebSocket connection to be
 * alive so that the backend can forward messages from other
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
 * If the backend detects a dropped connection, it cannot
 * reconnect, but it can send a message to other connected users,
 * to warn them of the situation.
 *
 * Restoring connectivity means checking regularly if messages to
 * the backend are being acknowledged. The frequency of these
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
 *    be resent with the same corr(elation id), so that the backend
 *    can ignore it if it was already treated (idempotency).
 *
 * The heartbeat delay will be calculated dynamically based on the
 * longest response time over the past few cycles, and the current
 * state (IDLE or WAITING).
 *
 * ////////////////////////////////////////////////////////////// *
 * A separate WebSocket backend script handles these features:
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
 * id). Any response from the backend will include the same corr.
 *
 * Promises and rejection
 * ----------------------
 * The caller will receive a Promise. This will resolve if:
 *  + The message is acknowledged by the backend, and no rsvp
 *    timeout delay was given
 *  + The message was handled by the backend, which sent a custom
 *    response, using the same corr.
 *
 * The Promise will reject if the message could not be sent, did
 * not receive any acknowledgement from the backend, or did not
 * receive a requested rsvp message after a delay of
 * opts.abandonAfterMs (default: 2 minutes).
 *
 * Callers must therefore be ready to catch rejections.
 *
 * Idempotent requests
 * -------------------
 * If an outgoing request does not receive an acknowledgement from
 * the backend within a certain delay, the request will be resent.
 * This can lead to the backend receiving the same instruction
 * multiple times
 *
 * CAVEAT: All requests must be idempotent, to ensure that the
 * backend executes exactly the same actions each time and responds
 * with exactly the same message.
 *
 *
 * See the comments for the send() function itself for more
 * details.
 */



;(function (root){
  "use strict"

  const DEFAULTS = {
    path:            "/ws",
    // Heartbeat rates and RTT tolerance
    keepaliveMs:     30000, // when little traffic is expected
    pulseFloorMs:    1000,  // every second when expecting incoming
    pulseCeilMs:     15000, // allows 5000ms RTT on slow connection
    latencyWindow:   20,    // max length of latencies array
    ackTimeoutMs:    1000,  // adjusted depending on latencies
    pulseRTTFactor:  3,     // ACK expected after ackTimeoutMs x 3
    pulseMaxMisses:  2,     // force reconnection if ACK missed x 2
    // Staggering reconnection attempts
    reconnectBaseMs: 250,   // 500, 1000, 2000, 4000, 8000, 16000,
    reconnectMaxMs:  32000, // after 8 attempts or almost 32s
    // Delay after which to give up trying to send a message
    abandonAfterMs:  120000,
  }


  function createGameSocket(userOptions) {
    const opts = Object.assign({}, DEFAULTS, userOptions)
    if (!opts.url) {
      throw new Error("GameSocket: url is required")
    }


    // INTERNAL STATE //

    let socket       = null
    let generation   = 0 // number of sockets created so far
    let socket_id    = "" // will be set on "CONNECTION"
    let state        = "IDLE" // IDLE | WAITING | RECONNECTING
    let restate      = state // for after a reconnection
    let ackTimeoutMs = opts.ackTimeoutMs
    let reconnectMs  = opts.reconnectBaseMs // pause till reconnect
    let reconnectTmr = null
    let closedByUser = false

    // Debug only
    let pulses       = 0

    // Unique values provided by backend on login
    let user_id      = "" // unique _id from User database
    let user_name    = "" // human-readable name (not unique?)

    const queue      = new Map() // for messages to resend
    const latencies  = [] // most recent Round Trip Times
    const listeners  = {
      open:      new Set(),
      close:     new Set(),
      incoming:  new Set(), // emits for incoming non-PING
      pending:   new Set(), // emits "sent" and "acknowledged"
      state:     new Set(), // IDLE / WAITING / RECONNECTING
      error:     new Set(), // emits on socket error
      // Debugging: for logging specific non-socket activities
      warn:      new Set(),
      info:      new Set(),
    }


    // PUBLIC API //
    const api = {
      // Connection
      cpr,
      setURL,
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
      events: Object.keys(listeners)
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
          fn(payload, event)

        } catch (error) {
          console.error(error)
        }
      }
    }


    // LIFECYCLE //

    function cpr() {
      // If there is an active socket, sends a manual PING, which
      // will call _scheduleReconnect() after ackTimeoutMs if it
      // fails to be acknowledged. If there is no active socket,
      // ensures that reconnection will occur.
      if (socket) {
        _reschedulePulse(socket, 0)

      } else {
        _setState("RECONNECTING") // ignored if RECONNECTING now
      }
    }


    function setURL(url) {
      if (opts.url === url) {
        return // no change; leave socket in its current state
      }

      // Change the socket's url for next connection
      opts.url = url

      if (isConnected()) {
        // Close current socket and open a new one to the new url
        _abandonSocket(socket, 1000, "resetting url")
      } // else wait for a connect() command
    }


    function connect(url) {
      closedByUser = false
      _openSocket("connect()")
    }


    /**
     * Called by the user, perhaps at a moment when the socket is
     * already down or reconnecting. This will close down an OPEN
     * socket and cancel any in-progress scheduled reconnection
     */
    function disconnect() {
      closedByUser = true
      const reason = "user disconnect"
      const code = 1000
      _teardown(code, reason) // returns immediately if !socket

      // Clear any current reconnectTmr that might have just been
      // set by _scheduleReconnect
      _cancelReconnect()
      _setState("IDLE")

      // If socket is already null, no _gen field is available for
      // it. Emit a "close" event anyway to confirm that the socket
      // is guaranteed to be closed.
      _emit("close", {
        ...(socket && { generation: socket._gen }),
        code,
        reason,
        wasClean: true // user's orders
      })
    }


    /**
     * Sent by connect() and a timeout set in _scheduleReconnect()
     * @param {string} reason may be "connect()" or "reconnect
     *                 timer"
     */
    function _openSocket(reason) {
      // Ensure that any previous socket is closed; socket = null
      _teardown(1000, reason)

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

      ws.onopen      = (event) => _onOpen(ws, event)
      ws.onerror     = (event) => _onError(ws, event)
      ws.onclose     = (event) => _onClose(ws, event)
      ws.onmessage   = (event) => _onMessage(ws, event)

      pulses = 0

      // Adopt this ws as the currently active socket
      socket = ws
      _emit("info", {
        action: "_openSocket",
        generation: myGen,
        reason
      })

      _scheduleReconnect() // in case this attempt fails
    }


    /**
     * Called by _openSocket() and disconnect()
     * @param {number} code is always 1000, because the close
     *        action is always voluntary
     * @param {string} reason will be "reconnect timer" if the call
     *        came from _openSocket(), "user disconnect" if it came
     *        from disconnect()
     * @returns immediately if no socket is active
     *
     * Closes any existing socket and its timeout arrays; sets
     * socket = null,
     */
    function _teardown(code = 1000, reason) {
      if (!socket) { return }

      const ws = socket
      socket = null

      _clearTimers(ws)

      const active = ws.readyState === WebSocket.OPEN
                  || ws.readyState === WebSocket.CONNECTING
      if (active) {
        try {
          ws.close(code, reason)
        } catch { /* already closing */ }
      }
    }


    /**
     * @source Called by _teardown() and _onClose()
     * @param {socket} ws will be the most recently active
     *         WebSocket instance
     * @action Ensures that none of the currently active timeout
     *         instances will fire
     */
    function _clearTimers(ws) {
      // Cancel any currently scheduled PING
      clearTimeout(ws._pulseTimer)
      ws._pulseTimer = null

      // Cancel any timeouts that would trigger if an ACK message
      // is not received
      // ws.pending is { corr => { resolve, timer, reject }, ... }
      for (const { timer } of ws._pending.values()) {
        clearTimeout(timer)
      }
      ws._pending.clear()

      // Cancel any timeouts that would trigger if an expected
      // response to an rsvp message is not received
      for (const { timer } of ws._rsvp.values()) {
        clearTimeout(timer)
      }
      ws._rsvp.clear()
    }


    // SOCKET EVENTS //

    /**
     * Sent by a WebSocket instance when it learns that it has
     * connected to the backend
     * @param {socket} ws is the socket that just opened
     * @returns immediately if ws has already been passed over as
     *          the official socket instance
     */
    function _onOpen (ws) {
      if (ws !== socket) { return }

      _cancelReconnect()
      socket_id = "" // a new value will be sent on "CONNECTION"
      reconnectMs = opts.reconnectBaseMs // delay until next time

      // Adopt the state that the previous socket was using, by
      // default. As soon as the backend receivse a LOG_IN message
      // it will send the current game state, which may mean that
      // the socket state will need to be updated.
      _setState(restate)

      // Start the heartbeat for the socket at the rate determined
      // by state and the most recent RTT latencies.
      _reschedulePulse(ws)

      _emit("open", {
        generation: ws._gen,
        url: ws.url
      })
    }


    function _onError(ws) {
      if (ws !== socket) {
        // ws has been politely trying to tell the backend that it
        // is closing because it's dead, but because it's dead, the
        // communication has failed, and the attempt has timed out
        // It's possible that ws was two generations ago.
        return
      }

      _emit("error", {
        generation: ws._gen,
        current: socket?._gen,
        url: ws.url
      })
    }


    /**
     * Called by a web socket when it has done its best to inform
     * the backend that it is closing. If the connection is broken,
     * this may be several seconds after socket.close() was called.
     * @param {socket} ws
     * @param {close event} event
     * @returns
     */
    function _onClose (ws, event) {
      if (ws !== socket) {
        // See notes for _onError() above
        return
      }

      // Prevent any current timeouts from firing
      _clearTimers(ws)

      // Make it impossible to send outgoing messages
      socket = null
      socket_id = ""

      const status = {
        generation: ws._gen,
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
      }

      _emit("close", status)
      if (closedByUser) {
        _setState("IDLE")
        return
      }

      _setState("RECONNECTING")
    }


    /**
     * Called when the socket receives an incoming message from
     * the backend.
     * @param {socket} ws
     * @param {string} message should be a JSON string with a
     *                 { ..., data, ... } field. The data value
     *                 is expected to have a format like:
     *                 {
     *                   "sender_id": "SYSTEM" | <uuid string>,
     *                   "recipient_id": <this socket_id>,
     *                   "subject": "ACK" | <custom subject>,
     *                   // Relevant if response to client request
     *                   "corr": <thread uuid>,
     *                   "time": <ms used to calculate RTT>,
     *                   // More?
     *                   [other fields]
     *                 }
     *
     * There are three types of incoming messages:
     * 1. Unsolicited messages, such as the "CONNECTION" sent by
     *    the backend when this socket connection comes online,
     *    messages from third-party clients, or messages broadcast
     *    to all clients in a specific group.
     * 2. "ACK" messages, sent by the backend immediately after it
     *    receives an outgoing message.
     * 3. Custom messages generated by the backend in response to
     *    an outgoing request.
     * NOTE: An outgoing request may be associated with an rsvp
     * timeout interval, or not).
     *
     * SYSTEM
     * ------
     * Messages with { sender_id: "SYSTEM", ... } are used to
     * update the internal state of this GameSocket instance. They
     * may also be listened for, so that the UI can be updated
     * appropriately.
     *
     * ACK
     * ---
     * This instance's `ackTimeouMs` value is set to a multiple of
     * the expected RTT for an immediate response from the backend.
     * If no response is received after that number of ms, one of
     * two things may have happened:
     *
     * 1. An "ACK" packet was sent but got lost
     * 2. The socket connection is broken
     * 3. The backend is down
     *
     * All outgoing messages are given a `corr` uuid and a `timer`
     * datetime number. The backend is expected to respond with an
     * "ACK" message with the same `corr` and `timer` values. If no
     * "ACK" message is received within ackTimeouMs, this instance
     * will test each of these hypotheses:
     *
     *  + A first missing "ACK" message is considered a glitch, and
     *    it is simply in the socket's `_ackMiss` property.
     *  + A subsequent missing "ACK" message is considered a sign
     *    that the socket connection is broken, so the current
     *    socket is torn down and replaced with a new one.
     *  + Staggered attempts will be made to create a new
     *    connection and if these fail for longer than expected,
     *    the UI will be warned that the backend may be down.
     *
     * Any "ACK" message that is received will set the socket's
     * `_ackMiss` property back to zero.
     *
     * A missed "ACK" may prompt this instance to resend the
     * original message with the same `corr` value and a new
     * `timer`. This can lead to the backend receiving multiple
     * identical requests.
     *
     *         **All requests must be idempotent.**
     *
     * RSVP
     * ----
     * You can call `send(payload, rsvp)` with an integer `rsvp`
     * value. This will start a timeout with the given `rsvp`
     * delay. If no custom response is received before the timeout
     * triggers, the Promise created for the outgoing message will
     * be rejected.
     *
     * A message with an `rsvp` will not automatically be resent
     * if its associated "ACK" is missed. (The missing "ACK" may
     * have been a glitch.) However, if multiple "ACK" messages are
     * missed in succession, this instance will create a new socket
     * and resend any messages that have not yet received a
     * response ("ACK" only, or `rsvp`).
     *
     * A message without an `rsvp` is not considered critical. If
     * no "ACK" or custom response is received for it, its promise
     * will resolve with a status (which may be "missed ACK"); it
     * will not reject.
     *
     * ENVELOPE
     * --------
     * All outgoing messages (except for no-ack single shot
     * messages) are associated with an envelope. The envelope
     * contains:
     *
     *  + The outgoing message
     *  + Its unique `corr` value
     *  + Its `time` (when first sent) value
     *  + A `timer` timeout index, identifying the timeout which
     *    will trigger if no "ACK" or `rsvp` response is received
     *    after a given delay
     *  + The resolve and reject callbacks for a promise. The
     *    reject callback will only be triggered for `rsvp`
     *    messages which fail to receive a timely custom response.
     *    The resolve callback will be triggered in all other
     *    cases:
     *    * Receipt of custom `rsvp` response
     *    * Receipt of "ACK" for non-rsvp messages
     *    * Failure to receive "ACK" for non-rsvp messages
     *
     * RESEND LIMIT
     * ---
     * A message an `rsvp` will continue to be resent until its
     * rsvp-defined timeout fires.
     *
     * A message without an `rsvp` will continue to be resent
     * until the timeout defined by `opts.abandonAfterMS` fires.
     *
     * NO-ACK SINGLE SHOT MESSAGES
     * ---
     * For certain messages, an "ACK" reply is overkill. For
     * instance, a message to update the client's cursor position
     * on a shared screen will be out of date almost immediately.
     *
     * For such messages, you can send an rsvp value of -1 (or
     * any value less than zero). No timeout will ever fire, and a
     * resolved Promise with { status: "shot", message } will be
     * returned immediately.
     *
     * _onMessage() may still receive responses to such messages,
     * but these will be handled as unsolicited third-party
     * messages.
     */
    function _onMessage(ws, { data }) {
      if (ws !== socket) {
        // ws is no longer trustworthy, and the incoming message
        // may be stale, or may duplicate the response to a
        // resent message. Let the new socket be the only source of
        // truth.
        return
      }

      let message
      try { message = JSON.parse(data) } catch { return }
      let treated = false

      // Any inbound message proves the socket is alive
      ws._ackMiss = 0

      // Wait a while before sending a new pulse message
      _reschedulePulse(ws)

      // Handle ACK messages separately (acknowledgement, latency)
      if (message.subject === "ACK" && message.corr) {
        return _settleAck(ws, message)
      }

      // Handle SYSTEM messages internally
      if (message.sender_id === "SYSTEM") {
        // subject = CONNECTION, LOG_IN, ...?
        treated = _treatSystemMessage(message)
      }

      // Check if the incoming message is the response to an
      // outgoing request from this client, with an rsvp
      const envelope = queue.get(message.corr)

      if (envelope && !treated) {
        _treatResponse(ws, message, envelope)

      } else if (!treated) { // ...by _treatSystemMessage()
        // Neither ACK nor response nor unshared SYSTEM; possibly
        // a message from a third party. If message.corr exists,
        // this will belong to the third party. If this is an
        // incoming Chat message, message.corr can be added to the
        // outgoing message as message.sender_corr, when confirming
        // that this user has opened the message.
        _emit("incoming", message)
      }
    }


    // HOUSEKEEPING // HOUSEKEEPING // HOUSEKEEPING //

    function _treatSystemMessage(message) {
      switch (message.subject) {
        case "CONNECTION":{
          socket_id = message.recipient_id // private and temporary

          // This message wasn't explicitly requested by a send()
          // action, so there is no Promise pending.
          _emit("incoming", {...message, url: socket.url})

          // The socket is now ready to send outgoing messages,
          // such as a LOG_IN reminder to the backend of this
          // user's identity.
          _identifyUser()
          break // status available through isConnected()
        }

        case "LOG_IN": {
          _handleLogIn(message)
          break
        }
      }

      return true // tell _onMessage() this was already treated
    }


    function _handleLogIn(message) {
      const envelope = queue.get(message.corr)

      // console.log(`LOG_IN ${message.corr} envelope: ${JSON.stringify(envelope)}`)
      if (!envelope) {
        // Not one of ours, or already treated
        return
      }

      // Response received; no need to resend
      clearTimeout(envelope.ackTimer) // might already be cleared
      clearTimeout(envelope.endTimer)
      queue.delete(message.corr)

      // console.log(`LOG_IN envelope${envelope}`)


      // Store user_id and user_name locally. These can be
      // accessed through gameSocket.getUser()
      if (message.status === "LOGGED_IN") {
        ({ user_id, user_name } = message)

        // Now that the backend can identify the user to whom
        // this socket belongs, it's safe to send any
        // previously unacknowledged messages.
        _sendMessagesInQueue()

      } else {
        // LOGIN_FAILED: perhaps the previously connected
        // user has been expelled.
        user_name = user_id = ""
      }

      // .then() and .catch() are in external script for
      // initial LogIn call, or _identifyUser() on reconnection
      envelope.resolve({
        status: message.status,
        message: envelope.message // original outgoing message
      })

      _emit("pending", {
        corr: message.corr,
        message, // contains status, user_name and user_id
        status: "handled"
      })
    }


    /**
     * Called by _treatSystemMessage() when "CONNECTION" is
     * received. If the previous connection in this session was
     * broken, the backend needs to be told which user the new
     * socket belongs to, so that it can apply any instructions
     * from this user appropriately.
     * The backend will respond to this request with a LOGGED_IN
     * message, at which point any queued messages that did not
     * receive acknowledgement before the socket broke will be
     * resent.
     */
    function _identifyUser() {
      if (user_name || user_id) {
        const rsvp = 2000
        promise = send({
          recipient_id: "SYSTEM",
          subject: "LOG_IN",
          user_id,
          user_name
        }, rsvp)
        .then(response => (
          _emit("info", response)
        ))
        .catch(error => (
          _emit("info", error)
        ))
      }
    }


    // STATE MACHINE //

    function isConnected (socket_id_required) {
      return !!((!socket_id_required || socket_id)
               && socket
               && socket.readyState === WebSocket.OPEN
               )
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
        _reschedulePulse(socket)
      }

      if (next === "RECONNECTING") {
        // The connection was dropped and should soon reopen
        _scheduleReconnect()
      }

      _emit("state", { from: prev, to: next })
    }


    // KEEPALIVE / IDLE (slow) / WAITING (fast)

    /**
     * Sent by:
     *  + cpr
     *  + _onOpen
     *  + _onMessage
     *  + _setState
     *  + _treatMissedACK
     * @param {socket} ws
     * @returns immediately if ws is not the active socket
     * Prevents any currently scheduled PING from being sent, and
     * schedules one at the appropriate later time.
     */
    function _reschedulePulse(ws, delay) { // undefined || 0
      if (ws !== socket) { return }

      clearTimeout(ws._pulseTimer)

      const pulse = () => send({ subject: "PING" })
      if (isNaN(parseInt(delay)))  {
        delay = _pulseInterval()
      }
      ws._pulseTimer = setTimeout(pulse, delay)
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


    function _getAckTimeout() {
      if (latencies.length < 3) {
        return opts.ackTimeoutMs
      }

      const sorted = [...latencies].sort((a, b) => a - b)
      const p95    = sorted[Math.floor(sorted.length * 0.95)]

      return p95 * opts.pulseRTTFactor
    }


    // RECONNECT //

    /**
     * Sent when _setState("RECONNECTING") is called, which occurs
     * in _onClose() and _reschedulePulse() after an ACK response
     * is missed. If the _openSocket() attempt fails, _onClose()
     * will be called again, but reconnectMs will have increased,
     * to give the backend progressively more time to react.
     */
    function _scheduleReconnect () {
      _cancelReconnect()

      if (reconnectMs === opts.reconnectMaxMs) {
        // The most recent connection attempt happened after a
        // series of increasingly longer delays, now totalling the
        // longest expected delay. It looks like the backend may be
        // offline.
        const delay = Math.floor(opts.reconnectMaxMs / 1000)
        const message = `Unable to connect to backend after ${delay}s.`
        _emit("warn", { issue: "connection", message })
        // This warning will be repeated every reconnectMaxMs ms
        // until a connection is established or the user
        // disconnects.
      }

      reconnectTmr = setTimeout(() => {
        reconnectTmr = null
        _openSocket("reconnect timer")
      }, reconnectMs)

      // Wait longer before next reconnect attempt
      reconnectMs = Math.min(opts.reconnectMaxMs, reconnectMs * 2)

      _emit("info", { action: "_scheduleReconnect", reconnectMs })
    }


    /**
     * Called by _scheduleReconnect() and (manually) disconnect()
     */
    function _cancelReconnect() {
      clearTimeout(reconnectTmr)
      reconnectTmr = null
    }


    // SENDING //

    /**
     * See the notes for _onMessage()
     * @param {object} payload must be an object
     * @param {mixed} rsvp may be an integer, which should be
     *         bigger than ackTimeoutMs. If it is < 0, then no
     *         acknowledgment will be expected. If it is falsy,
     *         no custom reply from the backend will be expected.
     * @returns a Promise that will be resolved or rejected with an
     *         object like {
     *           message: payload
     *           status: "shot"|"acknowledged"|"handled"|"unknown"
     *         }
     *         If rsvp < 0, the Promise will already be resolved
     *         with status: "shot".
     * If rsvp < 0: Attempts to send the payload immediately
     * Otherwise: Places payload in an envelope and prepares to
     *         send it (and resend it if necessary) when a socket
     *         is available.
     */
    function send(payload, rsvp) {
      if (typeof payload !== "object") {
        throw new Error("Object required as payload for send")
      }
      rsvp = parseInt(rsvp) || 0 // NaN is falsy

      if (rsvp < 0) {
        // Single shot. No acknowledgement or custom response is
        // expected. Example: sending the client's mouse position.
        socket?.send(JSON.stringify(payload))
        return Promise.resolve({
          message: payload,
          status: "shot"
        })
      }

      return new Promise((resolve, reject) => {
        // Prepare message for sending, and queue it, regardless
        // of whether the socket is able to send it yet
        const corr  = crypto.randomUUID()
        const message = {
          corr,
          ...payload
        }

        // console.log("send() - corr:", corr, payload.subject)

        const envelope = {
          corr,
          message,
          resolve,
          reject,
          rsvp
        }

        // PING messages are only sent once; they are not queued
        if (message.subject !== "PING") {
          // This is a user action message. Prepare to send it
          // multiple times.
          queue.set(corr, envelope)
          _emit("pending", { corr, message, status: "queued" })
        } else {
          // DEBUGGING ONLY
          // _emit("info", { corr, subject: message.subject })

        }

        if (!isConnected(true)) {
          if (message.subject === "PING") {
            // Drop the PING; it should never be queued. Nothing is
            // awaiting the Promise, so resolve value is arbitrary.
            resolve("socket not ready")
          }
          return // wait for the next socket to open
        }

        _sendQueuedMessage(socket, envelope) // even PINGs, once
      })
    }


    /**
     * Called by send(), _retrySend(), _sendMessagesInQueue()
     * @param {socket} ws should be the current socket
     * @param {object} envelope should be an object with format {
     *          message, // object
     *          corr,    // unique id string
     *          resolve, // Promise fulfiller
     *          reject,  // Promise fulfiller
     *          rsvp,    // positive intereger or undefined
     *          retries  // integer
     *        }
     * @returns immediately if the message can't be sent
     */
    function _sendQueuedMessage(ws, envelope) {
      if (!socket_id || ws.readyState !== WebSocket.OPEN) {
        // This call will be made again when a connection is ready
        return
      }

      const { message, corr, rsvp } = envelope // in queue if !PING
      // Ensure message has an official socket_id (first-time only)
      message.sender_id = socket_id

      if (!envelope.endTimer && message.subject !== "PING") {
        // Prepare to reject this message if its deadline has
        // passed. (Not relevant for PINGs)
        const delay = rsvp ? rsvp : opts.abandonAfterMs
        envelope.endTimer = setTimeout(() => (
            _hitDeadline(ws, corr)
          ), delay)
        }

      // Create a timeout for the ACK message (for every resend)
      envelope.ackTimer = setTimeout(() => (
        _treatMissedACK(ws, corr, message.subject)
      ), ackTimeoutMs)

      // Prepare to calculate RTT for this attempt, for latency
      envelope.time = Date.now()

      // envelope is already stored on queue, as:
      // {
      //   message,  // outgoing message
      //   corr,     // its unique id
      //   resolve,  // Promise resolve
      //   reject,   // Promise reject
      //   rsvp,     // falsy, or positive integer
      //   ackTimer, // index for timeout to trigger _missedACK()
      //   endTimer, // index for timeout to trigger _hitDeadline()
      //   time      // local ms when ws.send() was about to occur
      // }

      // Store a pointer to envelope in the ws instance
      ws._pending.set(corr, envelope)

      ws.send(JSON.stringify(message))
      // One of two things will now happen:
      // 1. An incoming ACK message with corr will indicate that
      //    the connection was still alive and the backend received
      //    the message
      // 2. The timeout will trigger, suggesting a connection
      //    issue. This can happen up to pulseMaxMisses times for
      //    a given socket before a new socket connection is
      //    scheduled.
      //
      // PING messages will never be resent.
      // For user-initiated messages, a missed ACK will force the
      // message to be resent with the same socket. If
      // pulseMaxMisses has been reached for this socket, a new
      // connection will be scheduled, and the message will be
      // resent with the new socket.
      //
      // A user-initiated message may also expect a custom rsvp
      // response. This will be identified by the same corr, but
      // may have any subject and payload.
      //
      // NOTE: The entire cycle, resend + ACK (+ rsvp) must occur
      // within the lifetime of a single socket, before the
      // endTimer fires.

      if (message.subject !== "PING") {
        // Indicate message has been sent, but not yet ACK'd
        _emit("pending", { corr, message, status: "sent" })
      }
    }


    /**
     * Sent by the backend immediately after receiving an outgoing
     * message.
     * @param {socket} ws is the socket that sent the message
     * @param {object} message should have the structure: {
     *          subject:      "ACK",
     *          recipient_id: outgoing sender_id,
     *          corr:         outgoing corr,
     *          time:         ms when outgoing message was sent
     *        }
     * @returns immediately if missedACK timeout already fired
     * It's possible that this or an earlier ACK message arrived
     * late, so _treatMissedACK() was triggered.
     */
    function _settleAck (ws, message) { // incoming message
      const { corr } = message

      // console.log("SETTLE:", corr, message.subject)

      // Check if message is still pending or expecting an rsvp
      // (PINGs will be in ws._pending but not in queue)
      const envelope = ws._pending.get(corr)

      // DEBUGGING
      // _emit("info", { _settleAck: ">>>>>", envelope })

      if (!envelope || !envelope.ackTimer) {
        // The outgoing message fully handled earlier, or an
        // earlier ACK message was treated for an rsvp message
        return
      }

      const {
        rsvp,
        ackTimer,
        endTimer,
        resolve,
        time
      } = envelope

      // No longer pending acknowledgement: clear the ACK timeout
      // and update the envelope shared by ws._pending and queue
      clearTimeout(ackTimer)
      envelope.ackTimer = false
      // console.log(`ackTimer ${ackTimer} cleared for ${corr} ${message.subject}`)


      _recordLatency(time, corr, envelope.message.subject) // corr is just for debugging

      // Update the pending status for all messages except PINGs
      if (envelope.message.subject === "PING") {
        return _cleanUpPing(ws, envelope)

      } else {
        _emit("pending", {
          corr,
          message: envelope.message,
          status: "acknowledged"
        })
      }

      // Check if there's an rsvp to wait for before resolving
      // the promise created by send()
      if (!rsvp) {
        // Clean up now that the ACK-only message has been treated
        clearTimeout(endTimer) // so it doesn't fire
        ws._pending.delete(corr) // no longer waiting for anything
        queue.delete(corr) // action complete: no need to resend

        resolve({
          status: "acknowledged",
          message: message // original outgoing message
        })

      } // else {
          // Don't resolve or remove from queue until rsvp has
          // been dealt with one way or the other
      //}
    }


    function _recordLatency (time, corr, subject) {
      // corr and subject only for debugging
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

      ++pulses
      _emit("info", `${pulses}: ACK ${ms} ${corr.slice(0, 8)} ${subject}`)
    }


    /**
     * Triggered by the timeout identified by envelope.ackTimer
     * when an incoming "ACK" message was not received for corr
     * after ackTimeoutMs
     * @param {socket} ws is the socket the message was sent by
     * @param {string} corr identifies the outgoing message: PING
     *        or user-initiated message
     */
    function _treatMissedACK(ws, corr, subject) {
      // Ensure that _settleACK will not run if the actual ACK
      // response arrives after its deadline.
      const envelope = ws._pending.get(corr)
      const { message, time } = envelope
                    // ^^^^ time only for debugging "warn" below
      _emit("warn", `MISSED ACK for ${subject} ${corr.slice(0, 8)} after ${Date.now() - time} ms`)

      const isPING = message.subject === "PING"
      if (isPING) {
        // Tidy up for garbage collection
        _cleanUpPing(ws, envelope)
        // Send a new PING to check if this was just a glitch
        _reschedulePulse(ws, 0)

      } else {
        // The immediate ACK message did not arrive in time, but
        // this could have been a glitch. It might arrive still
        // arrive before its deadline.
        // If this happens for a non-rsvp message, the outgoing
        // eventually message succeeded. A late "acknowledged"
        // status can be emitted, and the Promise can be
        // resolved.
        // If this happens for an rsvp message, there are still
        // two chances for success during the lifetime of this
        //  current socket: a late ACK message and a timely rsvp.

        // Action: Leave the envelope on both queue and ws._pending.

        // Provide a progress update
        _emit("pending", { corr, message, status: "overdue ACK" })
      }

      // This missed ACK is a sign that the socket may already
      // have broken. If it continues a sequence of other
      // missed ACK messages, this socket will be abandoned, and
      // the whole send + ack (+ rsvp) process will start again
      // with a new socket.

      if (ws !== socket) {
        // A new socket has already taken control. The envelope is
        // still in queue and will be resent when a new socket is
        // ready
        return
      }

      // Resend the message (hoping for an ACK this time), or
      // initiate reconnection depending on whether this one
      // missed ACK was just a glitch.

      ws._ackMiss++
      console.log("ws._ackMiss:", ws._ackMiss, socket_id)
      if (ws._ackMiss >= opts.pulseMaxMisses) {
        _abandonSocket(ws, 1000, "reconnecting")
        // ws._pending will be cleared, and all envelopes in queue
        // will be resent when socket opens. If a delayed ACK
        // message arrives now, _onMessage() will ignore it,
        // because sender ws !== current socket

      } else if (!isPING) {
        // Use the same socket to resend the message with a new
        // ackTimer, to generate a new ACK message. Note that the
        // earlier ACK message may still be on its way, so it's
        // possible that multiple ACK messages will arrive for the
        // same corr.
        _sendQueuedMessage(ws, envelope) // use the same socket
      }
    }


    function _cleanUpPing(ws, envelope) {
      envelope.resolve("clean up PING") // nothing is awaiting this
      ws._pending.delete(envelope.corr)

      // DEBUGGING
      // _emit("info", { cleanup: true, envelope })
    }


    /**
     * Called by _onMessage() for messages that requested an rsvp
     * from the backend
     * @param {socket} ws is the socket the message was sent by
     * @param {string} message will be the incoming response (not
     *                 the original outgoing request message)
     * @param {object} receipt will be { corr, timer }, where timer
     *                 is the value for timeout that will trigger
     *                 after rsvp ms, as provided to send(). It was
     *                 read from ws._rsvp.
     */
    function _treatResponse(ws, message, envelope) {
      if (!envelope) {
        // A previous rsvp message for this corr has been treated
        return
      }

      const {
        message: outgoing,
        corr,
        resolve,
        ackTimer,
        endTimer
      } = envelope

      // Clean up now that rsvp has been received
      clearTimeout(endTimer)
      queue.delete(corr)
      ws._pending.delete(corr)

      _emit("info", `${message.subject} ${corr.slice(0,8)} in _treatResponse()`)

      // It's possible that the ACK message sent immediately by the
      // has not arrived yet. Consider this receipt also as an
      // acknowledgement
      if (ackTimer){
        clearTimeout(ackTimer)
        _emit("pending", { corr, message, status: "acknowledged" })
      }

      // Now that the outgoing message has been acted on, resolve
      // the Promise created by send()
      resolve({
        status: "rsvp received",
        message: outgoing // original outgoing message in envelope
      })

      // The calling script can use corr to associate this response
      // with the outgoing message whose progress it was informed
      // about in previous "pending" updates.
      _emit("pending", {
        corr,
        message, // current incoming message containing a response
        status: "handled"
      })
    }


    /**
     * Triggered by the timeout identified by envelope.endTimer,
     * whose duration will be rsvp (if there is one) or
     * abandonAfterMs
     * @param {socket} ws is the socket the message was sent by
     * @param {string} corr identifies the outgoing user-
     *         initiated message (not applicable to PINGs)
     * @returns
     */
    function _hitDeadline(ws, corr) {
      const envelope = queue.get(corr)
      if (!envelope) { return }

      const{ message, ackTimer, rsvp, resolve } = envelope
      queue.delete(corr)
      ws._pending.delete(corr)
      clearTimeout(ackTimer)
      // clearTimeout(endTimer) // already triggered

      const status = rsvp ? "unknown" : "no ACK"

      _emit("pending", { corr, message, status })
      resolve({ message, status })
    }


    /**
     * Called by _treatSystemMessage() after it treats a
     * successful incoming LOG_IN message, confirming that the
     * backend knows which user this new socket belongs to.
     * Iterates through the messages in queue to resend them.
     */
    function _sendMessagesInQueue() {
      // Debugging >>>>>>>
      const queued = []
      queue.forEach( a => (
        queued.push(`"${a.message.subject}", "${a.corr.slice(0,8)}…"\n`)
      ))
      _emit("info", `_sendMessagesInQueue: [\n ${queued}]`)
      // <<<<<<<

      const now = Date.now()

      for (const [corr, envelope] of [...queue]) {
        if (now > envelope.abandonAfter) {
          _hitDeadlin(socket, corr)
          continue
        }

        _sendQueuedMessage(socket, envelope)
      }
    }


    function _abandonSocket(ws, code=1000, reason="abandoned") {
      if (ws !== socket || state === "RECONNECTING") {
        return // already torn down, or stale
      }

      // Detach the socket from the client's state immediately to
      // prevent any attempt to send outgoing messages on a dead
      // socket. Any incoming messages that _do_ arrive will still
      // be handled by the socket that sent them (but this is
      // unlikely since that socket has just been reported dead).
      socket = null
      socket_id = ""
      _clearTimers(ws)
      // New timeouts will be created by _sendQueuedMessage() after
      // _sendMessagesInQueue()

      // Tell the dead socket politely to close. This will cause
      // it to attempt to communicate its intention to close with
      // the backend. Such attempts are almost bound to fail and
      // will result in an error after the socket has given the
      // backend time to respond, but it didn't. `onerror` will be
      // called at that point, but it will be old news and can be
      // ignored.
      try {
        if ( ws.readyState === WebSocket.OPEN
          || ws.readyState === WebSocket.CONNECTING
        ) {
          ws.close(code, reason)
        }
      } catch { /* already closing */ }

      // Emit the close event ourselves, so listeners see a close
      // immediately rather than waiting for the browser to realize
      // that the socket is dead and officially call ws.close().
      // Such a "close" event on a dead socket will be ignored.
      _emit("close", {
        generation: ws._gen,
        code,
        reason,
        wasClean: false,
        synthetic: true, // distinguishes from a real browser close
      })

      _setState("RECONNECTING")
    }
  }


  // ── Export ──────────────────────────────────────────────────────
  if (typeof module !== "undefined" && module.exports) {
    module.exports = { createGameSocket }
  } else {
    root.GameSocket = { createGameSocket }
  }
})(typeof globalThis !== "undefined" ? globalThis : this)