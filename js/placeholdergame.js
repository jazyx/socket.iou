/**
 * frontend/js/placeholdergame.js
 */

// Set up GameSocket and its Recorder from globals
const parent     = document.body
const backend    = document.getElementById("backend")
const gameSocket = GameSocket.createGameSocket({
  url: backend.value,
})
const eventHandler = {}
Recorder({
  ...gameSocket,  // { on, events, ... }
  parent,
  custom: customRecords(),
  eventHandler,
  autoOpen: true
})
gameSocket.connect()

;(function ({
  on,
  cpr,
  setURL,
  connect,
  disconnect,
  send,
  startWaiting,
  stopWaiting
}, eventHandler){

  // Steal the handleEvent() function from Recorder, if possible
  if ( typeof eventHandler === "object"
    && typeof eventHandler.handleEvent === "function"
  ) {
    eventHandler = eventHandler.handleEvent
  } else {
    eventHandler = null
  }

  const frontend   = document.getElementById("frontend")

  const connected  = document.getElementById("connected")
  const buttons    = document.getElementById("buttons")
  const toggleWait = document.getElementById("toggleWait")
  const start      = document.getElementById("connect")
  const stop       = document.getElementById("disconnect")

  const form       = document.getElementById("form")
  const username   = document.getElementById("username")
  const messages   = document.getElementById("messages")
  const rsvp       = document.getElementById("rsvp")

  let waiting = false
  let counter = 0

  frontend.textContent = location.origin

  backend.addEventListener("click", chooseURL)
  start.addEventListener("click", () => connect())
  stop.addEventListener("click",  () => disconnect())
  form.addEventListener("submit", logIn)
  toggleWait.addEventListener("click", toggleWaiting)
  rsvp.addEventListener("click", requestResponse)

  // Listen for connection status
  on("open", showConnectionStatus)
  on("close", showConnectionStatus)

  function showConnectionStatus(data, event) {
    const isConnected = event === "open"
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


  function chooseURL({target}) {
    setURL(target.value)
  }


  function logIn(event) {
    event.preventDefault()

    if (!username.value) { return }

    send({
      subject: "LOG_IN",
      recipient_id: "SYSTEM",
      user_name: username.value
    }, 1000)
    .then(response => (
      log("logIn resolved", response)
    ))
    .catch(error => (
      log("logIn rejected", error)
    ))
  }


  function toggleWaiting(param) {
    waiting = !waiting
    if (waiting) {
      startWaiting()
      toggleWait.textContent = "Is WAITING"
    } else {
      stopWaiting()
      toggleWait.textContent = "Is IDLE"
    }
  }


  function requestResponse() {
    const promise = send({
      subject: "RSVP",
      text: `Message ${counter++}`,
    }, 2000)
    // log("promise:", promise)
    promise.then(response => {
        log("resolved", response)
      })
      .catch(error => {
        log("REJECTED", error)
      }
    )
  }


  function log(status, data) {
    if (eventHandler) {
      eventHandler(data, status)
    } else {
      console.log(status, data)
    }
  }
})(gameSocket, eventHandler)