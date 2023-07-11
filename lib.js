import 'dotenv/config'
import { ethers } from 'ethers'
import { createConnection } from 'node:net'

export const socketPath = process.env.SOCKET || '/tmp/rockettree.ipc'

export function socketCall(request) {
  const socket = createConnection({path: socketPath, allowHalfOpen: true, noDelay: true})
  socket.setEncoding('utf8')
  const data = []
  socket.on('data', (d) => data.push(d))
  return new Promise(resolve => {
    socket.on('end', () => resolve(data.join('')))
    socket.end(request.join('/'))
  })
}

export const cachedCall = (contractName, fn, args, blockTag) =>
  socketCall(['contract', contractName, fn, args.join(), blockTag])

const verbosity = parseInt(process.env.VERBOSITY) || 2
export const log = (v, s) => verbosity >= v ? console.log(s) : undefined

const genesisTimes = new Map()
genesisTimes.set('mainnet', 1606824023n)
genesisTimes.set('goerli', 1616508000n)

export function tryBigInt(s) { try { return BigInt(s) } catch { return false } }

export const startBlock = parseInt(process.env.START_BLOCK) || 'latest'

export const provider = new ethers.JsonRpcProvider(process.env.RPC_URL || 'http://localhost:8545')
export const networkName = await provider.getNetwork().then(n => n.name)
export const genesisTime = genesisTimes.get(networkName)

export const secondsPerSlot = 12n
export const slotsPerEpoch = 32n

export const stakingStatus = 2

const max64 = 2n ** 64n
export function uint256To64s(n) {
  const uint64s = []
  for (const _ of Array(4)) {
    uint64s.push(n % max64)
    n >>= 64n
  }
  return uint64s
}
export function uint64sTo256(a) {
  let n = 0n
  while (a.length) {
    n <<= 64n
    n += a.pop()
  }
  return n
}
export const addressToUint64s = s => uint256To64s(BigInt(s))
export const uint64sToAddress = a => ethers.getAddress(`0x${uint64sTo256(a).toString(16).padStart(40, '0')}`)

export function makeLock() {
  const queue = []
  let locked = false

  return function execute(fn) {
    return acquire().then(fn).then(
      r => {
        release()
        return r
      },
      e => {
        release()
        throw e
      })
  }

  function acquire() {
    if (locked)
      return new Promise(resolve => queue.push(resolve))
    else {
      locked = true
      return Promise.resolve()
    }
  }

  function release() {
    const next = queue.shift()
    if (next) next()
    else locked = false
  }
}
