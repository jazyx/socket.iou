function customRecords() {
  return {
    open: ({ data, event, details, summary, addEntry }) => {
      const { url, generation } = data
      summary.textContent = `open: ${url} (${generation})`

      addEntry({ data, event, details, summary })
    },


    close: ({ data, event, details, summary, addEntry }) => {
      const { reason } = data
      summary.textContent = `close: ${reason}`

      addEntry({ data, event, details, summary })
    },


    incoming: ({ data, event, details, summary, addEntry }) => {
      const { subject } = data
      summary.textContent = `incoming: ${subject}`

      addEntry({ data, event, details, summary })
    },

    pending: ({ data, event, details, summary, addEntry }) => {
      let { corr, status, message } = data
      corr = corr.slice(0, 8)
      summary.textContent = `pending: ${corr} ${status} ${message.subject}`
      let bg = ""
      switch (status) {
        case "queued":
          bg = "#840"
        break;
        case "sent":
          bg = "#860"
        break;
        case "acknowledged":
          bg = "#880"
        break;
        case "handled":
          bg = "#080"
        break;
      }
      summary.style.backgroundColor = bg

      addEntry({ data, event, details, summary })
    },

    state: ({ data, event, details, summary, addEntry }) => {
      const { from, to } = data
      summary.textContent = `state: ${from} —> ${to}`

      addEntry({ data, event, details, summary })
    },

    error: ({ data, event, details, summary, addEntry }) => {
      if (typeof data === "string") {
        summary.textContent = data
        summary.style.backgroundColor = "#900"
        data = ""
        details.setAttribute("open", true)
      }

      addEntry({ data, event, details, summary })
    },

    warn: ({ data, event, details, summary, addEntry }) => {
      if (typeof data === "string") {
        summary.textContent = data
        summary.style.backgroundColor = "#960"
        data = ""
        details.setAttribute("open", true)
      }

      addEntry({ data, event, details, summary })
    },

    info: ({ data, event, details, summary, addEntry }) => {
      if (typeof data === "string") {
        summary.textContent = data
        details.setAttribute("open", true)

        if (/^(\d+: )?ACK /.test(data)) {
          summary.style.backgroundColor = "#009"
        }

        data = ""
      }

      addEntry({ data, event, details, summary })
    }
  }
}