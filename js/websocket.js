/**
 * frontend/js/websocket.js
 */


const WS_PATH = "/ws"
const MIN_DELAY = 250    // 500 1000 2000 4000
const MID_DELAY = 4000
const MAX_DELAY = 32000  // 4000 8000 16000 32000
const TIMEOUT   = 2000


;(function (){
  const div = document.getElementById("websocket")
  const connected  = div.querySelector(".connected")
  const buttons    = div.querySelector(".buttons")
  const connect    = div.querySelector(".connect")
  const disconnect = div.querySelector(".disconnect")
  const form       = div.querySelector("form")
  const username   = div.querySelector(".username")
  const messages   = div.querySelector(".messages")

  const initialMS = + new Date()
  let socket  = null
  let isConnected  = false
  let user_id = ""
  let restartMS = 0
  let lastPing

  let ping_interval
  let ping_timeout = 0
  let ping_delay = MID_DELAY
  let down = 0
  let max = 0
  let min = 99999
  const latencies = []
  const uptimes = []


  connect.addEventListener("click", openSocket)
  disconnect.addEventListener("click", closeSocket)
  form.addEventListener("submit", logIn)


  function resetServer(event) {
    if (event) {
      url = event.target.value
    }

    console.log("WebSocket resetServer url:", url + WS_PATH)

    if (isConnected && socket) {
      socket.close(1000, "reconnecting")
      socket = null
    }

    socket = openSocket(url)
  }


  function openSocket() {
    if (isConnected) { return } // already connected, ignore
    if (socket && socket.readyState === WebSocket.OPEN) { return }

    socket = new WebSocket(url + WS_PATH)
    console.log("openSocket:", socket)
    addMessageToList("openSocket request")

    socket.onopen    = treatConnection
    socket.onerror   = treatError
    socket.onmessage = treatMessage
    socket.onclose   = treatDisconnect

    restartMS = + new Date()
    if (down) {
      const message = `Down time: ${restartMS - down} ms`
      console.log("message:", message)
      addMessageToList(message)
    }
    ping_interval = setInterval(ping, ping_delay)
    socket.ping_interval = ping_interval

    return socket
  }


  function closeSocket(code=1000, reason="client action") {
    const message = `socket.close("${code}", "${reason}") called ${socket ? "for "+socket.user_id : "on null socket"}`

    if (socket) {
      const oldSocket = socket
      socket = null
      isConnected = false

      oldSocket.close(code, reason)
      const { user_id, ping_interval } = oldSocket
      clearInterval(ping_interval)
    }

    console.log(message)
    addMessageToList(message)
  }


  function treatConnection(event) {
    addMessageToList(`"${event.type}" event received`)
    isConnected = true
    showConnectionStatus()

    username.select()
    username.focus()
  }


  function treatError(event) {
    const message = `socket.error() called at ${new Date().toTimeString().slice(0, 8)} ${socket ? "for "+socket.user_id : "on null socket"}\nisConnected: ${isConnected}\nsocket.readyState: ${socket ? socket.readyState : "no socket"}`

    console.log(message)
    addMessageToList(message)

    showConnectionStatus()
  }


  function treatMessage({ data }) {
    try {
      const message = JSON.parse(data)
      handleMessage(message)

    } catch(error) {
      console.warn(`ERROR: data could not be converted to an object\n°${data}°`)
    }

    showConnectionStatus()
  }


  function treatDisconnect(event) {
    console.log("disconnect:", event)
    const { target, code, reason, wasClean } = event
    const { user_id, ping_interval } = (target || {
      user_id: "unknown",
      ping_interval: -1
    })

    const message = `"${event.type}" event received for ${user_id} (ping_interval: ${ping_interval})\ncode: ${code}, reason: "${reason}", wasClean: ${wasClean}, uptime: ${uptimes.slice(-1)[0] || 0}s`
    console.log(message)
    addMessageToList(message)

    isConnected = false
    clearInterval(ping_interval)

    showConnectionStatus()
  }


  // Messages // Messages // Messages // Messages // Messages //

  function handleMessage(message) {
    const { sender_id, recipient_id, subject } = message

    switch (sender_id) {
      case "SYSTEM":
        return handleSystemMessage(message)

    }

    // Other messages
    console.log(`handleMessage(${JSON.stringify(message, null, 2)})`)
  }


  function handleSystemMessage(message) {
    const { recipient_id, subject } = message

    switch (subject) {
      case "CONNECTION":
        user_id = recipient_id
        socket.user_id = recipient_id.slice(0, 8)
        console.log(`user_id set to ${user_id}`)
        addMessageToList(message)

      break

      case "PONG":
        return handlePongMessage(message)
    }
  }


  function sendMessage(message) {
    if (typeof message !== "object") { return }
    // Server cannot treat a message that does not have a subject
    // or a recipient_id key/value pair.

    if (
         !socket
      || socket.readyState !== WebSocket.OPEN
      || !user_id
    ) {
      // The socket must exist and be open, and the server requires
      // a sender_id in order to reply.
      console.warn(
        "WebSocket FAILED TO SEND MESSAGE\n",
        message,
        "state:", socket?.readyState,
        user_id
      )
      return -1
    }

    message.sender_id = user_id
    // console.log("Sending message:", message)

    message = JSON.stringify(message)
    // console.log(`sendMessage(${message})`)

    socket.send(message)

    return 0
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
    messages.append(li)

    messages.scroll(0, messages.scrollHeight)
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


  function showConnectionStatus() {
    connected.textContent = "" + isConnected
    const action = isConnected ? "remove" : "add"
    buttons.classList[action]("disconnected")
    username.classList[action]("disconnected")
  }


  // Ping // Ping // Ping // Ping / Ping // Ping // Ping // Ping //

  function ping() {
    ping_timeout = setTimeout(timeoutPing, TIMEOUT)
    lastPing = +new Date()

    const message = {
      recipient_id: "SYSTEM",
      subject: "PING",
      time: lastPing,
      ping_timeout
    }

    const failure = sendMessage(message)

    if (failure) {
      console.log("PING message not sent:", message)
    }
  }


  function timeoutPing() {
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
    // happens at the halfway point: after (ping_delay / 2) ms
    const uptime = Math.round(
      (   lastPing              // time of failed attempt
        - (ping_delay / 2)      // halfway point before that
        + statistics.midLatency // time alive after last success
        - restartMS             // time resetServer was called
      ) / 100
    ) / 10
    uptimes.push(uptime)

    statistics.uptime = uptime
    statistics.down_at = down.toTimeString().slice(0, 8)

    statistics = JSON.stringify(statistics, null, 2)
    console.log("statistics:", statistics)
    addMessageToList(statistics)

    let uptimeInfo = getStatistics(uptimes, "Uptime")
    const total = Math.round((+ new Date() - initialMS) / 100) / 10
    uptimeInfo.totalTime = total
    uptimeInfo.ratio = Math.round(
      uptimeInfo.totalUptime * 1000 / total
    ) / 10 + "%"

    uptimeInfo = JSON.stringify(uptimeInfo, null, 2)
    console.log("uptimeInfo:", uptimeInfo)
    addMessageToList(uptimeInfo)

    // Stop pinging until the socket is reopened
    clearInterval(ping_interval)

    // Consider that the connection was dropped and restart it.
    closeSocket(1000, "ping timeout")
    resetServer()
  }


  function getStatistics(array, label="") {
    const max = Math.max.apply(null, array)
    const min = Math.min.apply(null, array)
    const length = array.length
    const total = Math.round(array.reduce((sum, value) => (
      sum += value
    )) * 10) / 10
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


  function handlePongMessage(message) {
    const { ping_timeout, time } = message

    // If the message was received before the timeout was triggered
    // then don't trigger it
    clearTimeout(ping_timeout)

    const latency = (+ new Date() - time)
    latencies.push(latency)

    message = `Ping ${ping_timeout} latency: ${latency}`
    console.log("pong", message);

    addMessageToList(message)
  }


  resetServer()
  showConnectionStatus()
})()