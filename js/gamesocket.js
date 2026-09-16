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
 * + AWAITING incoming messages
 * + ACTIVE (making moves)
 *
 * This WebSocket client will:
 *
 *  + Send a heartbeat message every 25-30 seconds in IDLE mode
 *    to ensure the connection remains open
 *  + Probe the server every few seconds in AWAITING mode
 *  + Request an ACK(nowledgement) of all outgoing messages when in
 *    ACTIVE mode, and will reconnect and resend any messages that
 *    do not receive an ACK response within a reasonable time.
 *
 * The timeout delay will be calculated dynamically based on the
 * longest response time over the past few cycles.
 *
 * Constant "probe" messages will drain the battery of a mobile
 * device, so these are only sent:
 *
 *  - If there have been recent disconnections
 *  - When the user is passively AWAITING incoming messages
 *
 * On a stable network, a disconnection may occur unexpectedly
 * after a long period of tranquillity. The slow heartbeat messages
 * will eventually pick up on this, but other players may be
 * unable to continue the game until it is fixed. The game
 * interface should include a Restore Connection button that the
 * user can press if notified by other players of the need.
 *
 * The probe frequency will restart high after any disconnection,
 * but will slow down if the connection remains stable for a long
 * time.
 *
 * ////////////////////////////////////////////////////////////// *
 * A separate WebSocket server script handles these features:
 *
 *  + Detecting when a client connection breaks, and warning other
 *    connected users
 *  + Maintaining a centralised game state that is updated by all
 *    incoming player actions
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
    keepaliveMs:     30000,
    probeFloorMs:    2000,
    probeCeilMs:     15000,
    probeRttFactor:  3,
    probeMaxMisses:  2,
    reconnectBaseMs: 1000,
    reconnectMaxMs:  30000,
    latencyWindow:   20,
    ackTimeoutMs:    5000,

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
    let state        = "IDLE"     // IDLE | AWAITING | RECONNECTING
    let reconnectMs  = opts.reconnectBaseMs
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
      message:   new Set(),
      state:     new Set(),
      reconnect: new Set(),
    }


    // PUBLIC API //
    const api = {
      setURL,
      connect,
      disconnect,
      // Messages
      send,
      // State
      enterAwaiting,
      exitAwaiting,
      getState:    () => state,
      isConnected,
      // Listeners
      on,
      off,
    }
    return api



    // EVENT EMITTERS //

    function on(event, fn) {
      if (!listeners[event]) {
        // "open"
        // "close"
        // "error"
        // "message"
        // "state"
        // "reconnect"
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

    function setURL() {
      // NOT REQUIRED ON VOYAGE; exercise for the reader
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
      _teardown(reason, 1000)

      const myGen = ++generation
      const ws    = new WebSocket(opts.url + opts.path)

      ws._gen        = myGen
      ws._ackTimer   = null
      ws._probeTimer = null
      ws._keepTimer  = null
      ws._probeMiss  = 0
      ws._pending    = new Map()

      ws.onopen      = (e) => _onOpen(ws, e)
      ws.onerror     = (e) => _onError(ws, e)
      ws.onclose     = (e) => _onClose(ws, e)
      ws.onmessage   = (e) => _onMessage(ws, e)

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
      clearTimeout(ws._probeTimer)
      clearTimeout(ws._keepTimer)
      ws._ackTimer = ws._probeTimer = ws._keepTimer = null

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
      _scheduleKeepalive(ws)
      _emit("open", { generation: ws._gen })

      opts.c.showStatus(isConnected())
    }


    function _onError () {

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


    function _onMessage (ws, { data }) {
      if (ws !== socket) { return }

      let msg
      try { msg = JSON.parse(data) } catch { return }

      // Any inbound message proves liveness.
      ws._probeMiss = 0
      clearTimeout(ws._probeTimer)
      ws._probeTimer = null
      _scheduleKeepalive(ws)

      // Handle ACK and SYSTEM messages only internally
      if (msg.subject === "ACK" && msg.corr) {
       return  _settleAck(ws, msg)

      } else if (msg.sender_id === "SYSTEM") {
        // CONNECTION, LOGGED_IN, ...?
        const share = _treatSystemMessage(msg)
        if (!share) {
          return
        }
      }

      // Neither ACK nor unshared SYSTEM
      _emit("message", msg)
    }


    function _treatSystemMessage(message) {
      switch (message.subject) {
        case "CONNECTION":
          socketId = message.recipient_id
          opts.c.log("socketId", `${socketId.slice(0, 8)}…`)
          break

        case "LOGGED_IN":
          ({ user_id, user_name } = message)
          opts.c.log(`user_name: ${user_name}${user_id ? ", user_id: "+user_id : ""}`)
          return true
      }

      return false
    }


    // STATE MACHINE //

    function isConnected() {
      return !!socket && socket.readyState === WebSocket.OPEN
    }


    function _setState () {

    }


    function enterAwaiting() {

    }


    function exitAwaiting() {

    }


    // KEEPALIVE / IDLE

    function _scheduleKeepalive(ws) {
      clearTimeout(ws._keepTimer)

      const confirmAlive = () => {
        if (ws !== socket) { return }

        ws._keepTimer = null

        send({ subject: "PING" }, "keepAlive")
        _scheduleKeepalive(ws)
      }

      ws._keepTimer = setTimeout(confirmAlive, opts.keepaliveMs)
    }


    // PROBE / AWAITING

    function _scheduleProbe () {

    }


    function _probeInterval () {

    }


    // RECONNECT //

    function _scheduleReconnect () {

    }


    function _cancelReconnect () {

    }


    // SENDING //

    function send(payload, label, timeoutMs = opts.ackTimeoutMs) {
      if (typeof label === "number") {
        timeoutMs = label
        label = ""
      }

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
        }, timeoutMs)

        socket._pending.set(corr, { resolve, timer, reject })
        const message = {
          ...payload,
          sender_id: socketId,
          corr,
          time: Date.now(),
        }

        _sendRaw(socket, message)

        opts.c.log(label || "outgoing", message)
      })
    }


    function _settleAck (ws, msg) {
      const pending = ws._pending.get(msg.corr)
      if (!pending) { return }

      ws._pending.delete(msg.corr)
      clearTimeout(pending.timer)

      if (typeof msg.time === "number") {
        _recordLatency(msg.time)
      }

      pending.resolve(msg)
    }


    function _sendRaw (ws, msg) {
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        // Was already checked in _send(), so not applicable
        return false
      }

      ws.send(JSON.stringify(msg))
      return true
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