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

  let socket  = null
  let isConnected  = false
  let user_id = ""
  let startMS = 0

  let ping_interval
  let ping_timeout = 0
  let ping_delay = MID_DELAY
  let down = 0
  const latencies = []


  connect.addEventListener("click", openSocket)
  disconnect.addEventListener("click", closeSocket)
  form.addEventListener("submit", logIn)


  function setServer(event) {
    if (event) {
      url = event.target.value
    }

    console.log("WebSocket setServer url:", url + WS_PATH)

    if (isConnected && socket) {
      socket.close()
    }

    socket = openSocket(url)
  }


  function openSocket() {
    if (isConnected) { return } // already connected, ignore
    if (socket && socket.readyState === WebSocket.OPEN) { return }

    socket = new WebSocket(url + WS_PATH)
    console.log("openSocket:", socket)

    socket.onopen    = treatConnection
    socket.onerror   = treatError
    socket.onmessage = treatMessage
    socket.onclose   = treatDisconnect

    startMS = + new Date()
    if (down) {
      console.log(`Down time: ${startMS - down} ms`)
    }
    ping_interval = setInterval(ping, ping_delay)

    return socket
  }


  function closeSocket(code=1000, reason="client action") {
    socket.close(code, reason)
    console.log(`socket.close(${code}, ${reason}) called`)
  }


  function treatConnection(event) {
    addMessageToList(`"${event.type}" event received`)
    isConnected = true
    showConnectionStatus()

    username.select()
    username.focus()
  }


  function treatError(event) {
    console.log("ERROR:", event)
    addMessageToList(`"${event.type}" event received \nisConnected: ${isConnected}\nsocket.readyState: ${socket ? socket.readyState : "no socket"}`)

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
    const { code, reason, wasClean } = event
    const uptime = Math.round((+ new Date() - startMS) / 100) / 10

    addMessageToList(`"${event.type}" event received\ncode: ${code}, reason: "${reason}", wasClean: ${wasClean}, uptime: ${uptime}s`)
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

    const message = {
      recipient_id: "SYSTEM",
      subject: "PING",
      time: + new Date(),
      ping_timeout
    }

    const failure = sendMessage(message)

    if (failure) {
      console.log("PING message not sent:", message)
    }
  }


  function timeoutPing() {
    // A ping message was not answered in time. Show the statistics
    const maxLatency = Math.max.apply(null, latencies)
    const minLatency = Math.min.apply(null, latencies)
    const length = latencies.length
    const midLatency = length
      ? latencies.reduce((sum, value) => (
          sum += value
        )) / latencies.length
      : "n/a"

    down = new Date()

    const statistics = JSON.stringify({
      maxLatency,
      midLatency,
      minLatency,
      length,
      down: d.toTimeString().slice(0, 8)
    }, null, '  ')
    console.log("statistics:", statistics)
    addMessageToList(statistics)

    // Stop pinging until the socket is reopened
    clearInterval(ping_interval)
    
    // Consider that the connection was dropped and restart it.
    closeSocket(1000, "ping timeout")
    setServer()
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


  setServer()
  showConnectionStatus()
})()