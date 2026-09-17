/**
 * frontend/js/gameconsole.js
 *
 * Deals with the interface.
 */


;(function (root){
  // Elements
  const frontend   = document.getElementById("frontend")
  const backend    = document.getElementById("backend")

  const connected  = document.getElementById("connected")
  const buttons    = document.getElementById("buttons")
  const toggleWait = document.getElementById("toggleWait")
  const start      = document.getElementById("connect")
  const stop       = document.getElementById("disconnect")
  const form       = document.getElementById("form")
  const username   = document.getElementById("username")
  const messages   = document.getElementById("messages")

  frontend.textContent = location.origin


  function createSocketConsole() {
    let connect      = () => {}
    let disconnect   = () => {}
    let send         = () => {}
    let startWaiting = () => {}
    let stopWaiting  = () => {}
    let on           = () => {}
    let off          = () => {}
    const cancel = {

    }

    start.addEventListener("click", () => connect())
    stop.addEventListener("click",  () => disconnect())
    form.addEventListener("submit", logIn)
    toggleWait.addEventListener("click", toggleWaiting)

    let waiting = false


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


    function logIn(event) {
      event.preventDefault()

      if (!username.value) { return }

      send({
        subject: "LOG_IN",
        recipient_id: "SYSTEM",
        user_name: username.value
      })
    }


    // PUBLIC API //

    return {
      url:        backend.value,
      adopt:      adoptSocket,
      addToList:  addMessageToList,
      showStatus: showConnectionStatus,
      log,
      logCall:    logMethodCall,
      statistics: showStatistics,
    }



    function adoptSocket(api) {
      ({
        connect,
        disconnect,
        send,
        startWaiting,
        stopWaiting,
        on,
        off
      } = api)

      // Events to listen for:
      // "open"
      // "close"
      // "error"
      // "message"
      // "state"
      // "reconnect"

      // cancel["message"] = on("message", handleMessage)
    }


    function handleMessage(message) {
      // if (message.subject === "ACK") { return }

      log("incoming", message)
    }


    /**
     * @param {boolean} isConnected 
     */
    function showConnectionStatus(isConnected) {
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


    function logMethodCall(event, method) {
      const { type, target } = event
      const { socket_id, readyState } = target
      if (type) {
        method = `socket.on${type}()`
      }

      let time = getTime()
                  
      const message = `${method} called at ${time} for ${socket_id}\nisConnected: ${isConnected}\nsocket.readyState: ${readyState}`

      log(message)
    }


    function log(label, data) {
      if (data === "#now") {
        data = getTime()
      }

      if (!data) {
        console.log(label)
        addMessageToList(label)
      } else {
        console.log(label, data)
        addMessageToList(label, data)
      }
    }


    function truncate(key, value) {
      if (typeof value === "string" && value.length > 9) {
        value = `${value.slice(0, 8)}…`
      }
      
      return value 
    }


    function addMessageToList(label, data) {
      if (!data) {
        data = label
        label = ""
      }

      switch (typeof data) {
        case "string": 
        case "number": 
          break
        case "boolean":
          data = "" + data
          break
        case "object":
          data = JSON.stringify(data, truncate, '  ')
          break
        default:
          data = `${data} (${typeof data})`
      }

      const li = document.createElement("li")
      li.textContent = `${label ? label+": " : ""}${data}`

      const scrolledToBottom = messages.scrollHeight
                              - messages.scrollTop
                              - messages.clientHeight
                              < 8 // arbitrary
      messages.append(li)

      if ( scrolledToBottom ) {
        messages.scroll(0, messages.scrollHeight)
      }
    }


    function showStatistics(latencies, uptimes) {
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
      const max = extreme(array)
      const min = extreme(array, "min")
      const length = array.length
      const total = sum(array)
      const mid = length
        ? Math.round(total * 10 / length) / 10
        : 0

      const statistics = Object.entries(
        { max, mid, min, length, total }
      )
      return statistics.reduce((output, [key, value]) => {
        output[key+label] = value
        return output
      }, {})
    }


    const extreme = (array, limit="max") => (
      array.length ? Math[limit].apply(null, array) : 0
    )


    function sum(array) {
      if (!array.length) {
        return 0
      }

      return Math.round(array.reduce((sum, value) => (
        sum += value
      )) * 10) / 10
    }


    function getTime() {
      const time = new Date()
      return time.toTimeString().slice(0, 8)
          + "."
          + time.getMilliseconds()
    }
 }


  
  // ── Export ──────────────────────────────────────────────────────
  if (typeof module !== "undefined" && module.exports) {
    module.exports = { createSocketConsole }
  } else {
    root.SocketConsole = { createSocketConsole }
  }
})(typeof globalThis !== "undefined" ? globalThis : this)