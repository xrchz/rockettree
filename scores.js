import { socketCall, cachedCall, genesisTime, secondsPerSlot, log } from './lib.js'
import { parentPort, threadId } from 'node:worker_threads'

const nodeSmoothingTimes = new Map()
async function getNodeSmoothingTimes(nodeAddress) {
  if (nodeSmoothingTimes.has(nodeAddress))
    return nodeSmoothingTimes.get(nodeAddress)
  const result = await socketCall(['nodeSmoothingTimes', nodeAddress])
  const optInTime = BigInt(result.optInTime)
  const optOutTime = BigInt(result.optOutTime)
  const value = {optInTime, optOutTime}
  nodeSmoothingTimes.set(nodeAddress, value)
  return value
}

async function processAttestation({minipoolAddress, slotIndex}) {
  const blockTime = genesisTime + secondsPerSlot * BigInt(slotIndex)
  const nodeAddress = await cachedCall(minipoolAddress, 'getNodeAddress', [], 'finalized')
  const {optInTime, optOutTime} = await getNodeSmoothingTimes(nodeAddress)
  if (blockTime < optInTime || blockTime > optOutTime) return
  const statusTime = BigInt(await cachedCall(minipoolAddress, 'getStatusTime', [], 'finalized'))
  if (blockTime < statusTime) return
  const currentBond = BigInt(await cachedCall(minipoolAddress, 'getNodeDepositBalance', [], 'targetElBlock'))
  const currentFee = BigInt(await cachedCall(minipoolAddress, 'getNodeFee', [], 'targetElBlock'))
  const previousBond = BigInt(await cachedCall(
    'rocketMinipoolBondReducer', 'getLastBondReductionPrevValue', [minipoolAddress], 'targetElBlock'))
  const previousFee = BigInt(await cachedCall(
    'rocketMinipoolBondReducer', 'getLastBondReductionPrevNodeFee', [minipoolAddress], 'targetElBlock'))
  const lastReduceTime = BigInt(await cachedCall(
    'rocketMinipoolBondReducer', 'getLastBondReductionTime', [minipoolAddress], 'targetElBlock'))
  const {bond, fee} = lastReduceTime > 0 && lastReduceTime > blockTime ?
    {bond: previousBond, fee: previousFee} :
    {bond: currentBond, fee: currentFee}
  const minipoolScore = (BigInt(1e18) - fee) * bond / BigInt(32e18) + fee
  parentPort.postMessage({minipoolAddress, minipoolScore})
}

parentPort.on('message', async (msg) => {
  if (msg === 'exit') process.exit()
  processAttestation(msg)
  parentPort.postMessage('done')
})
