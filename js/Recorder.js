/**
 * js/Recorder.js
 *
 * Always show Open button. Place Close button inside mount.
 */



;(function (root){
  "use strict"


  const DEFAULTS = {
    maxEntries: 40,
    id: "recorder-ui"
  }


  let mount
  let bar
  let toggle
  let save
  let clear
  let list
  let mounted = false
  let scrollTop = 0
  const history = []


  function Recorder({
    on,
    events,
    maxEntries,
    parent,
    id,
    custom,
    eventHandler,
    autoOpen
  }) {
    // Check arguments
    const errors = []

    if (typeof on !== "function") {
      errors.push("on function expected in Recorder")
    }
    if (!Array.isArray(events) || events.some(event => (
      typeof event !== "string"
    ))) {
      errors.push("array of string event names expected in Recorder")
    }
    if (!(parent instanceof HTMLElement))
      errors.push("HTML parent element expected in Recorder")

    if (errors.length) {
      throw new Error(errors.join(", "))
    }

    // Default fallbacks
    if (!parseInt(maxEntries)) {
      maxEntries = DEFAULTS.maxEntries
    }

    if (typeof id !== "string") {
      id = DEFAULTS.id
    }

    if (typeof custom !== "object") {
      custom = {}
    }

    if (typeof eventHandler === "object") {
      eventHandler.handleEvent = handleEvent
    }


    /** Create container with toggle button */
    ;(function createUI() {
      mount = document.createElement("div")
      mount.id = id
      bar = document.createElement("div")
      bar.className = "bar"
      const label = document.createElement("label")
      label.className = "toggle-recorder"
      label.title = "Record events"
      toggle = document.createElement("input")
      toggle.type = "checkbox"
      toggle.addEventListener("change", toggleRecorder)
      label.append(toggle)
      bar.append(label)
      mount.append(bar)

      parent.append(mount)

      if (autoOpen) {
        toggle.checked = true
        initialize()
      }
    })()


    function initialize() {
      // Start listening for Global events
      events.forEach(event => (
        on(event, handleEvent)
      ))
      mounted = true

      // Add Save and Clear buttons, and details list
      save = document.createElement("button")
      save.textContent = "Save"
      save.setAttribute("disabled", true)
      save.addEventListener("click", saveEvents)

      clear = document.createElement("button")
      clear.textContent = "Clear"
      clear.setAttribute("disabled", true)
      clear.addEventListener("click", clearEvents)

      list = document.createElement("div")
      list.className = "list"

      bar.append(clear)
      bar.append(save)
      mount.append(list)
    }


    function toggleRecorder({ target }) {
      const { checked } = target

      if (!mounted) {
        initialize()
      }

      if (!checked) {
        // Remember scroll status of list before closing
        scrollTop = list.scrollTop
      }

      // Hide or show Recorder
      const action = checked
        ? "remove"
        : "add"

      mount.classList[action]("hide")

      if (checked) {
        // Apply scroll status to open list
        list.scrollTop = scrollTop
      }
    }


    function clearEvents(params) {
      _trimEntries(true)
      history.push({ CLEARED: _timeWithMilliseconds() })
    }


    function saveEvents() {
      const fileName = `JSON-${_timeWithMilliseconds()}.json`
      const json = JSON.stringify(history, null, 2)
      const type = 'text/plain;charset=utf-8'
      const blob = new Blob([json], { type })
      const link = document.createElement('a')
      link.href = URL.createObjectURL(blob)
      link.download = fileName
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
    }


    // Registered listener

    function handleEvent(data, event) {
      _addToHistory(event, data)

      // Create details and summary elements with default text
      const details = document.createElement("details")
      const summary = document.createElement("summary")
      summary.textContent = event

      const customFunction = custom[event]
      if (customFunction) {
        // Apply addEntry only after running the custom function,
        // which may, in fact, fail to call addEntry()
        customFunction({ data, event, details, summary, addEntry })

      } else {
        addEntry({ data, event, details, summary })
      }
    }


    function addEntry({ data, event, details, summary }) {
      // summary may have been updated in a custom function

      // Show full JSON data...
      if (data) {
        const p = document.createElement("p")
        if (typeof data === "object") {
          data = JSON.stringify(data, null, 2)
        }
        p.textContent = data
        details.append(p)
      }

      // ... and scroll t new entry if already fully scrolled
      const scrolledToEnd = _scrolledToEnd()

      details.append(summary)
      list.append(details)
      // Delete earliest entries
      _trimEntries()

      if (scrolledToEnd) {
        list.scrollTop = list.scrollHeight
      }
    }


    // Private functions

    function _timeWithMilliseconds() {
      const time = new Date()
      let ms = time.getMilliseconds()
      ms = ms < 10 ? "00"+ms : ms < 100 ? "0"+ms : ms
      return `${time.toTimeString().slice(0, 8)}.${ms}`
    }


    function _addToHistory(event, data) {
      let time = new Date()
      let ms = time.getMilliseconds()
      ms = ms < 10 ? "00"+ms : ms < 100 ? "0"+ms : ms
      history.push({
        event,
        data,
        time: _timeWithMilliseconds()
      })

      clear.removeAttribute("disabled")
      save.removeAttribute("disabled")
    }


    function _scrolledToEnd() {
      return list.scrollHeight
           - list.scrollTop
           - list.clientHeight
           < 8 // arbitrary
    }


    function _trimEntries(all) {
      const entryCount = (all)
        ? 0
         :maxEntries

      while (list.childElementCount > entryCount) {
        list.removeChild(list.firstElementChild)
      }
    }
  }


  // ── Export ──────────────────────────────────────────────────────
  if (typeof module !== "undefined" && module.exports) {
    module.exports = Recorder
  } else {
    root.Recorder = Recorder
  }
})(typeof globalThis !== "undefined" ? globalThis : this)