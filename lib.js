import 'dotenv/config'
import { ethers } from 'ethers'
import { isMainThread, threadId, Worker, MessageChannel, workerData } from 'node:worker_threads'

export const { port1: cacheUserPort, port2: cachePort } = isMainThread ? new MessageChannel() : { port1: workerData.cacheUserPort }
export let cacheWorker
if (isMainThread) {
  cacheWorker = new Worker('./cache.js', {workerData: cachePort, transferList: [cachePort]})
  cacheUserPort.once('close', cachePort.close)
}

let nextRequestId = 0n
const requests = new Map()
function requestHandler({id, response, error}) {
  const requestId = `${id.threadId}-${id.nonce}`
  if (error !== undefined) {
    console.warn(`dropping ${requestId} with error: ${error}`)
  }
  else if (requests.has(requestId)) {
    const resolve = requests.get(requestId)
    requests.delete(requestId)
    resolve(response)
  }
}
if (cacheUserPort) cacheUserPort.on('message', requestHandler)
export function socketCall(request) {
  const requestId = {threadId, nonce: nextRequestId++}
  const promise = new Promise(resolve => requests.set(`${requestId.threadId}-${requestId.nonce}`, resolve))
  cacheUserPort.postMessage({id: requestId, request})
  return promise
}

export const multicall = (calls, blockTag) =>
  socketCall(['multicall', calls, blockTag])

const verbosity = parseInt(process.env.VERBOSITY) || 2

const timestamp = () => Intl.DateTimeFormat('en-GB',
  {hour: 'numeric', minute: 'numeric', second: 'numeric'})
  .format(new Date())

export const log = (v, s) => verbosity >= v && console.log(`${timestamp()}: ${s}`)

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

export const rpip30Interval = 18 // TODO: for testnet too?
export const denebEpoch = 269568 // TODO: ditto

const max64 = 2n ** 64n
export function uint256To64s(n, z) {
  const uint64s = []
  for (const _ of Array(z || 4)) {
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
export const addressToUint64s = s => uint256To64s(BigInt(s), 3)
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

export const oneEther = ethers.parseEther('1')
const twoEther = 2n * oneEther

function log2(x) {
  // console.time(`log2(${x})`)
  const exponent = BigInt(Math.floor(Math.log2(parseInt(x / oneEther))))
  let result = exponent * oneEther
  let y = x >> exponent
  if (y == oneEther) return result
  let delta = oneEther
  for (const i of Array(60).keys()) {
    delta = delta / 2n
    y = (y * y) / oneEther
    if (y >= twoEther) {
      result = result + delta
      y = y / 2n
    }
  }
  // console.timeEnd(`log2(${x})`)
  return result
}

export const ln = (x) => (log2(x) * oneEther) / 1442695040888963407n
