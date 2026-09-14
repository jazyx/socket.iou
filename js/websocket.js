/**
 * frontend/js/websocket.js
 */


const WS_PATH = "/ws"

;(function (){
  const div = document.getElementById("websocket")
  const connected  = div.querySelector(".connected")
  const buttons    = div.querySelector(".buttons")
  const connect    = div.querySelector(".connect")
  const disconnect = div.querySelector(".disconnect")
  const form       = div.querySelector("form")
  const username   = div.querySelector(".username")
  const messages   = div.querySelector(".messages")

  let socket
  let isConnected  = false
  let user_id


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

    return socket
  }


  function closeSocket() {
    socket.close()
    console.log("socket.close() called")
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
    addMessageToList(`"${event.type}" event received \nisConnected: ${isConnected}\nsocket.readyState: ${socket.readyState}`)
    showConnectionStatus()
  }


  function treatMessage({ data }) {
    try {
      const message = JSON.parse(data)
      handleMessage(message)
      addMessageToList(message)

    } catch(error) {
      console.warn(`ERROR: data could not be converted to an object\n°${data}°`)
    }

    showConnectionStatus()
  }


  function handleMessage(message) {
    const { sender_id, recipient_id, subject } = message
    console.log(`handleMessage(${JSON.stringify(message, null, 2)})`)

    switch (sender_id) {
      case "SYSTEM":
        return handleSystemMessage(message)
    }
  }


  function handleSystemMessage(message) {
    const { recipient_id, subject } = message

    switch (subject) {
      case "CONNECTION":
        user_id = recipient_id
        console.log(`user_id set to ${user_id}`)

        break
    }
  }


  function treatDisconnect(event) {
    console.log("disconnect:", event)
    addMessageToList(`"${event.type}" event received\n(code: ${event.code}, wasClean: ${event.wasClean})`)
    isConnected = false
    showConnectionStatus()
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
      return
    }

    message.sender_id = user_id
    // console.log("Sending message:", message)

    message = JSON.stringify(message)
    console.log(`sendMessage(${message})`)

    socket.send(message)
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


  setServer()
  showConnectionStatus()
})()