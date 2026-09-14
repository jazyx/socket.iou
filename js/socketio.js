/**
 * frontend/js/socketio.js
 */

;(function (){
  const div = document.getElementById("socket-io")
  const connected  = div.querySelector(".connected")
  const buttons    = div.querySelector(".buttons")
  const connect    = div.querySelector(".connect")
  const disconnect = div.querySelector(".disconnect")
  const form       = div.querySelector("form")
  const username   = div.querySelector(".username")
  const messages   = div.querySelector(".messages")

  let socket
  let isConnected  = false

  backend.addEventListener("change", setServer)

  function setServer(event) {
    if (event) {
      url = event.target.value
    }
    console.log("setServer url:", url)

    if (isConnected) {
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
  form.addEventListener("submit", logIn)


  function connectSocket() {
    const result = socket.connect()
    console.log("connectSocket:", result )
  }

  function disconnectSocket() {
    socket.disconnect()
  }

  function treatConnection(message) {
    message += `\nto ${getURL()}`
    addMessageToList("connection:", message)
    isConnected = true
    showConnectionStatus()

    username.select()
    username.focus()
  }

  function treatDisconnect(message) {
    message += `\nfrom ${getURL()}`
    addMessageToList("disconnect:", message)
    isConnected = false
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
    connected.textContent = "" + isConnected
    const action = isConnected ? "remove" : "add"
    buttons.classList[action]("disconnected")
    username.classList[action]("disconnected")
  }


  function getURL() {
    const { hostname, port, path } = socket.l
    const l = `${hostname}${port ? ":"+port : ""}${path}`
    return l
  }

  setServer()
  showConnectionStatus()
})()