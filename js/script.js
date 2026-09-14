/**
 * frontend/js/script.js
 */


const frontend  = document.getElementById("frontend")
const backend = document.getElementById("backend")
const links = Array.from(document.getElementsByTagName("a"))
const hashes = links.map(a => getHash(a))

let url = backend.value
frontend.textContent  = location.origin

// Handle the tab display
window.addEventListener("hashchange", updateTabs)


function getHash(a) {
  return a.href.replace(/^[^#]+/, "")
}


function updateTabs(event) {
  let hash = window.location.hash
  if (hashes.indexOf(hash) < 0) {
    // Neither tab has been manually selected. Use the default.
    hash = hashes[links.findIndex(a => (
      a.classList.contains("current")
    ))]
  }

  const a = document.querySelector(`[href="${hash}"]`)
  links.forEach(a => {
    const action = (hash === getHash(a))
      ? "add"
      : "remove"
    a.classList[action]("current")
  })
}


updateTabs()