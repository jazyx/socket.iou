/**
 * frontend/js/websocket.js
 */

  ;(function (){
  const div = document.getElementById("websocket")
  const connected  = div.querySelector(".connected")
  const buttons    = div.querySelector(".buttons")
  const connect    = div.querySelector(".connect")
  const disconnect = div.querySelector(".disconnect")
  const form       = div.querySelector("form")
  const username   = div.querySelector(".username")
  const messages   = div.querySelector(".messages")

  let isConnected  = false

  const HANDLED = [
    "connection",   
    "disconnect",  
    "logged_in"
  ]

  // socket.on("connection", treatConnection)
  // socket.on("disconnect", treatDisconnect)
  // socket.on("logged_in",  treatLogin)
  // socket.onAny(treatAnyMessage)


  // connect.addEventListener("click", connectSocket)
  // disconnect.addEventListener("click", disconnectSocket)
  // form.addEventListener("submit", logIn)


  // function connectSocket() {
  //   const result = socket.connect()
  //   console.log("connectSocket:", result )
  // }

  // function disconnectSocket() {
  //   socket.disconnect()
  // }

  function treatConnection(message) {
    addMessageToList("connection:", message)
    isConnected = true
    showConnectionStatus()

    username.select()
    username.focus()
  }

  function treatDisconnect(message) {
    addMessageToList("disconnect:", message)
    isConnected = false
    showConnectionStatus()
  }

  // function logIn(event) {
  //   event.preventDefault()
  //   socket.emit("login", username.value, loggedIn)
  // }

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
  }


  function showConnectionStatus() {
    connected.textContent = "" + isConnected
    const action = isConnected ? "remove" : "add"
    buttons.classList[action]("disconnected")
    // username.classList[action]("disconnected")
  }


  showConnectionStatus()
})()