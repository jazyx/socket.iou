/**
 * frontend/js/socketio.js
 */

;(function (){
  const div = document.getElementById("socket-io")
  const connected  = div.querySelector(".connected")
  const buttons    = div.querySelector(".buttons")
  const connect    = div.querySelector(".connect")
  const disconnect = div.querySelector(".disconnect")
  const stop       = div.querySelector(".stop")
  const form       = div.querySelector("form")
  const username   = div.querySelector(".username")
  const messages   = div.querySelector(".messages")

  let socket
  let status = "stopped"

  backend.addEventListener("change", resetSocket)

  function resetSocket(event) {
    if (event) {
      url = event.target.value
    }
    console.log("resetSocket url:", url)

    if (status === "connected") {
      socket.disconnect()
    }

    socket = io(url)

    socket.on("connection", treatConnection)
    socket.on("disconnect", treatDisconnect)
    socket.on("logged_in",  treatLogin)
    socket.onAny(treatAnyMessage)
  }


  const HANDLED = [
    "connection",
    "disconnect",
    "logged_in"
  ]


  connect.addEventListener("click", connectSocket)
  disconnect.addEventListener("click", disconnectSocket)
  stop.addEventListener("click", stopSocket)
  form.addEventListener("submit", logIn)


  function connectSocket() {
    if (!socket) {
      resetSocket()
    } else {
      const result = socket.connect()
      console.log("connectSocket:", result )
    }
  }

  function disconnectSocket() {
    socket.disconnect()
  }

  function stopSocket() {
    socket && socket.disconnect()
    socket = null
    addMessageToList("connection:", "socket stopped")
    status = "stopped"
    showConnectionStatus()
  }

  function treatConnection(message) {
    message += `\nto ${getURL()}`
    addMessageToList("connection:", message)
    status = "connected"
    showConnectionStatus()

    username.select()
    username.focus()
  }

  function treatDisconnect(message) {
    message += `\nfrom ${getURL()}`
    addMessageToList("disconnect:", message)
    status = "disconnected"
    showConnectionStatus()
  }

  function logIn(event) {
    event.preventDefault()
    socket.emit("login", username.value, loggedIn)
  }

  function loggedIn(loggedInData) {
    console.log("loggedInData:", loggedInData)
    const { subject, user, message } = loggedInData
    addMessageToList(subject, message)
    showConnectionStatus()
  }

  function treatLogin(message) {
    addMessageToList("logged_in:", message)
    showConnectionStatus()
  }


  function treatAnyMessage(message) {
    if (HANDLED.indexOf(message) < 0) {
      addMessageToList("any:", message)
      showConnectionStatus()
    }
  }


  function addMessageToList(label, message) {
    console.log(label, message)
    li = document.createElement("li")
    li.textContent = message
    messages.append(li)

    messages.scroll(0, messages.scrollHeight)
  }


  function showConnectionStatus() {
    connected.textContent = status

    const unclassed = (status === "connected")
      ? ["disconnected", "stopped"]
      : (status === "disconnected")
        ? ["connected", "stopped"]
        : ["connected", "disconnected"]

    unclassed.forEach(className => (
      buttons.classList.remove(className)
    ))

    buttons.classList.add(status)

    const action = (status === "connected") ? "remove" : "add"
    username.classList[action]("disconnected")
  }


  function getURL() {
    const { hostname, port, path } = socket.l
    const l = `${hostname}${port ? ":"+port : ""}${path}`
    return l
  }

  // resetSocket()
  showConnectionStatus()
})()