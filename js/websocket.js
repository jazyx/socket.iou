/**
 * frontend/js/websocket.js
 * 
 * Each time a socket is created, the server will create a unique
 * socket_id, in order to know which socket to use to communicate
 * with this client. This is not a permanent id that identifies
 * this particular user. It will change each time the connection
 * breaks.
 * 
 * For the purposes of this proof-of-concept, each user should log
 * in with a unique name. In a real project, the backend will
 * create a User record with a unique key and password, and a
 * possibly non-unique username.
 * 
 * On startup, `scheduler` is set to the value of a timeout that
 * will trigger the ping() function. This function will send a
 * "PING" message to the backend after a period with no incoming
 * or outgoing messages. That is, it will be rescheduled by any
 * incoming or outgoing messages.
 * 
 * Any outgoing message (even non-PING messages) will be given a
 * `pinger` and a `time` value. The backend will immediately 
 * respond with a "PONG" message, and may send other messages
 * later.
 * 
 */


const WS_PATH = "/ws"
const PING_DELAY = 4000
const PONG_DELAY = 200


;(function (){
  const div = document.getElementById("websocket")
  const connected  = div.querySelector(".connected")
  const buttons    = div.querySelector(".buttons")
  const connect    = div.querySelector(".connect")
  const disconnect = div.querySelector(".disconnect")
  const form       = div.querySelector("form")
  const username   = div.querySelector(".username")
  const messages   = div.querySelector(".messages")

  // Always active
  let socket       = null
  let isConnected  = false
  let socket_id = "" // set by backend for sending messages here
                     // Will be updated every time a new socket is
                     // opened.
  
  let scheduler    // value of timeout that triggers next ping()
  let pinger       // value of timeout that triggers missedPong()
  let lastPing     // time a "PING" (or proxy) was last sent

  // For showing the statistics
  let showWorking = true

  const initialMS = + new Date()
  let restartMS = 0
  let down = 0
  let max = 0
  let min = 99999
  const latencies = []
  const uptimes = []


  backend.addEventListener("change", resetSocket)
  connect.addEventListener("click", openSocket)
  disconnect.addEventListener("click", closeSocket)
  form.addEventListener("submit", logIn)


  /**
   * Sent by
   *   + the script having loaded
   *   + an "change" event from the backend selector
   *   + by missedPong()
   * @param {mixed} event will only have a value if triggered by
   *   the backend socket. In this case, the `url` will charge.
   * In all cases, the socket will be closed and disposed of (if
   * necessary) and a new socket will be created with the given url
   */
  function resetSocket(event) {
    if (event) {
      url = event.target.value
    }

    console.log("WebSocket resetSocket url:", url + WS_PATH)

    if (socket) {
      socket.close(1000, "reconnecting")
      socket = null
      isConnected = false
    }

    openSocket(url)
  }


  function openSocket() {
    if (isConnected) { return } // already connected, ignore
    if (socket && socket.readyState === WebSocket.OPEN) { return }

    socket = new WebSocket(url + WS_PATH)
    log("openSocket request")

    socket.onopen    = treatOpen
    socket.onerror   = treatError
    socket.onclose   = treatClose
    socket.onmessage = treatMessage

    if (showWorking) {
      restartMS = + new Date()
      if (down) {
        const message = `Down time: ${restartMS - down} ms`
        log(message)
      }
    }
  }


  // SOCKET EVENTS // SOCKET EVENTS // SOCKET EVENTS //

  /**
   * Sent by the socket when it receives on "open" event
   * @param {open event} event 
   */
  function treatOpen(event) {
    log(`"${event.type}" event received`)

    // Create a timeout to trigger ping() after PING_DELAY ms
    reschedulePing() // sets scheduler timeout value

    // Show the Connect and Disconnect buttons in the right colours
    showConnectionStatus(true)
  }


  /**
   * Sent by the socket when it receives an "error" event. This
   * will not contain much useful information, and it precedes an
   * unexpected "close" event
   * @param {error event} event 
   */
  function treatError({ type, target }) {
    const message = `socket.${type}() called at ${new Date().toTimeString().slice(0, 8)} for ${target.socket_id }\nisConnected: ${isConnected}\target.readyState: ${target.readyState}`

    log(message)
  }


  /**
   * Sent by the socket when it receives a "close" event. This may
   * be sent from the backend, from the use of the Disconnect
   * button, or from a missedPong() timeout.
   * @param {close event} event 
   */
  function treatClose(event) {
    const { target, code, reason, wasClean } = event

    if (showWorking) {
      const { socket_id, readyState } = target
      const id = `${socket_id} (${pinger})`

      const uptime = Math.round((new Date() - restartMS) / 100)/10
      const message = `socket.close() called at ${new Date().toTimeString().slice(0, 8)} for ${id}\nsocket.readyState: ${readyState}\ncode: ${code}, reason: "${reason}", wasClean: ${wasClean}\nisConnected: ${isConnected}, uptime: ${uptime}s`

      log(message)
    }

    // If the close event was unexpected, or was triggered by the
    // Disconnect button, an outgoing message may still be waiting
    // for a "PONG" echo. If the event was sent from missedPong(),
    // that timeout will already have been triggered
    clearTimeout(pinger)

    // Show the Connect and Disconnect buttons in the right colours
    showConnectionStatus(false)
  }


  function treatMessage({ data }) {
    // An incoming message is proof that the WebSocket connection
    // was working up until this moment. Cancel missedPong()
    // timeout, since it won't be needed, and reset the scheduler.
    clearTimeout(pinger)
    reschedulePing()

    try {
      const message = JSON.parse(data)
      handleMessage(message)

    } catch(error) {
      console.warn(`ERROR: data could not be converted to an object\n°${data}°`)
    }
  }


  // CUSTOM EVENTS // CUSTOM EVENTS // CUSTOM EVENTS //


  /**
   * Sent manually by the Disconnect button, or by missedPong()
   * @param {mixed} event will be a click event, only if the call
   *   came from the Disconnect button. It is ignored. 
   * @param {mixed} code will only have a value if sent by 
   *   missedPong(), in which case it will be 100
   * @param {mixed} reason will only have a value if sent by 
   *   missedPong(), in which case it will be "ping timed out"
   */
  function closeSocket(event, code=1000, reason="client action") {
    const message = `closeSocket("${code}", "${reason}") called ${socket ? "for "+socket.socket_id : "on null socket"}`

    log(message)

    if (socket) {
      // If called from Disconnect, an outgoing message may still
      // be waiting for a "PONG" echo. If called from missedPong()
      // that timeout will already have been triggered
      clearTimeout(pinger)

      socket.close(code, reason)
      // will trigger treatClose() and tell the backend
      socket = null
    }

    // missedPong() will now call restartSocket(), but a call from
    // Disconnect will leave the socket as null.

    showConnectionStatus(false)
  }


  // Messages // Messages // Messages // Messages // Messages //

  /**
   * 
   * @param {object} message 
   */
  function handleMessage(message) {
    const { sender_id, recipient_id, subject } = message

    // Any incoming message acts as a "PONG", in that it proves
    // that the connection was working an instant ago. It will
    // always receive an ACK(nowledgement)
    handleACKMessage(message)

    switch (sender_id) {
      case "SYSTEM":
        return handleSystemMessage(message)
    }

    // Other messages are not treated yet
    console.log(`handleMessage(${JSON.stringify(message, null, 2)})`)
  }


  function handleSystemMessage(message) {
    switch (message.subject) {
      case "CONNECTION":
        socket_id = message.recipient_id
        socket.socket_id = socket_id.slice(0, 8)
        log(`socket_id set to ${socket.socket_id}\n${JSON.stringify(message, null, 2)}`)

      break
      case "LOGGED_IN":
        // Cosmetic only in this version
        log(`username set to ${message.user_name}`)
      break
    }
  }


  function sendMessage(message) {
    if (typeof message !== "object") { return }

    if (
         !socket
       || socket.readyState !== WebSocket.OPEN
       || !socket_id
    ) {
      // The socket must exist and be open, and the server requires
      // a sender_id in order to reply.
      console.warn(
        "WebSocket FAILED TO SEND MESSAGE\n",
        message,
        "state:", socket?.readyState,
        socket_id
      )
      return -1
    }

    message.sender_id = socket_id

    // Ensure that each outgoing message acts as a PING proxy
    lastPing = +new Date()
    message.time = lastPing
    pinger = message.pinger = setTimeout(missedPong, PONG_DELAY)

    message = JSON.stringify(message)
    // console.log(`sendMessage(${message})`)

    socket.send(message)

    return 0
  }


  function logIn(event) {
    event.preventDefault()
    const user_name = username.value

    const message = {
      subject: "LOG_IN",
      recipient_id: "SYSTEM",
      user_name
    }

    sendMessage(message)
  }


  // Ping // Ping // Ping // Ping / Ping // Ping // Ping // Ping //

  /**
   * Sets a manual ping to trigger `PING_DELAY` milliseconds after
   * the last action that suggest the WebSocket is active. This
   * could be:
   * + After restartServer()
   * + After an incoming message is received
   */
  function reschedulePing() {
    clearTimeout(scheduler)
    scheduler = setTimeout(ping, PING_DELAY)
  }


  function ping() {
    // Send an "empty" message. The sendMessage() function will
    // add in:
    // + sender_id
    // + time
    // + pinger value
    sendMessage({ subject: "PING" }) // actually unnecessary
  }


  function missedPong() {
    if (showWorking) {
      showStatistics()
    }

    // Consider that the connection was dropped and restart it.
    closeSocket(null, 1000, "ping timed out")
    resetSocket()
  }


  function handleACKMessage(message) {
    const { pinger, time } = message

    // ALERT: messages broadcast from a third party MUST NOT have
    // a pinger value
    if (pinger) {
      clearTimeout(pinger)
    }
    
    // Get ready to send a new "PING" message if there is no other
    // traffic for a while
    reschedulePing()

    if (showWorking && time) {
      const latency = (+ new Date() - time)
      latencies.push(latency)

      message = `Ping ${pinger} latency: ${latency}`
      log(message)
    }
  }


  resetSocket()


  // HOUSEKEEPING / HOUSEKEEPING // HOUSEKEEPING / HOUSEKEEPING //

  function showConnectionStatus(status) {
    if (typeof status === "boolean") {
      isConnected = status
    }

    connected.textContent = "" + isConnected // "true" | "false"
    const action = isConnected ? "remove" : "add"

    // Show the Connect and Disconnect buttons in the appropriate
    // colours; disable the Log In field if disconnected.
    buttons.classList[action]("disconnected")
    username.classList[action]("disconnected")

    if (isConnected) {
      // Select the username
      username.select()
      username.focus()
    }
  }


  function log(data, force) {
    if (showWorking || force) {
      console.log(data)
      addMessageToList(data)
    }
  }


  function addMessageToList(data) {
    switch (typeof data) {
      case "string": break
      case "object":
        data = JSON.stringify(data, null, '  ')
        break
      default:
        data = `${data} (${typeof data})`
    }

    li = document.createElement("li")
    li.textContent = data

    const scrolledToBottom = messages.scrollHeight
                           - messages.scrollTop
                           - messages.clientHeight
                           < 4 // arbitrary
    messages.append(li)

    if ( scrolledToBottom ) {
      messages.scroll(0, messages.scrollHeight)
    }
  }


  function showStatistics() {
    // A ping message was not answered in time. Show the statistics
    let statistics = latencies.length
      ? getStatistics(latencies, "Latency")
      : {}

    if (max < (statistics.maxLatency || 0)) {
      max = statistics.maxLatency
    }
    if (min > (statistics.minLatency || 99999)) {
      min = statistics.minLatency
    }
    statistics.min = min
    statistics.max = max

    // Reset for the next unbroken stretch
    latencies.length = 0

    down = new Date()
    // The break could have happened any time in the period between
    // the last successful ping and lastPing. Let's say it always
    // happens at the halfway point: after (PING_DELAY / 2) ms
    const uptime = Math.round(
      (   lastPing              // time of failed attempt
        - (PING_DELAY / 2)      // halfway point before that
        + statistics.midLatency // time alive after last good ping
        - restartMS             // time resetSocket was called
      ) / 100
    ) / 10
    uptimes.push(uptime)

    statistics.uptime = uptime
    statistics.down_at = down.toTimeString().slice(0, 8)

    statistics = JSON.stringify(statistics, null, 2)
    log(statistics)

    let uptimeInfo = getStatistics(uptimes, "Uptime")
    const total = Math.round((+ new Date() - initialMS) / 100) / 10
    uptimeInfo.totalTime = total
    uptimeInfo.ratio = Math.round(
      uptimeInfo.totalUptime * 1000 / total
    ) / 10 + "%"

    uptimeInfo = JSON.stringify(uptimeInfo, null, 2)
    log(uptimeInfo)
  }


  function getStatistics(array, label="") {
    const max = Math.max.apply(null, array)
    const min = Math.min.apply(null, array)
    const length = array.length
    const total = sum(array)
    const mid = length
      ? Math.round(total * 10 / length) / 10
      : "n/a"

    const statistics = Object.entries(
      { max, mid, min, length, total }
    )
    return statistics.reduce((output, [key, value]) => {
      output[key+label] = value
      return output
    }, {})
  }


  const sum = (array) => {
    if (!array.length) {
      return 0
    }

    return Math.round(array.reduce((sum, value) => (
      sum += value
    )) * 10) / 10
  }
})()