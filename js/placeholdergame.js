/**
 * frontend/js/placeholdergame.js
 */


const socketConsole = SocketConsole.createSocketConsole()
const gameSocket = GameSocket.createGameSocket({
  url: socketConsole.url,
  c:   socketConsole
})
socketConsole.adopt(gameSocket)
gameSocket.connect()

