import { socketCall, cachedCall, genesisTime, secondsPerSlot, log } from './lib.js'
import { parentPort, workerData, threadId } from 'node:worker_threads'

const nodeSmoothingTimes = new Map()
async function getNodeSmoothingTimes(nodeAddress) {
  if (nodeSmoothingTimes.has(nodeAddress))
    return nodeSmoothingTimes.get(nodeAddress)
  const result = JSON.parse(await socketCall(['nodeSmoothingTimes', nodeAddress]))
  const optInTime = BigInt(result.optInTime)
  const optOutTime = BigInt(result.optOutTime)
  const value = {optInTime, optOutTime}
  nodeSmoothingTimes.set(nodeAddress, value)
  return value
}

async function processEpoch(epoch) {
  const value = await socketCall(['attestations', epoch]).then(s => s.split(','))
  const minipoolScores = new Map()
  while (value.length) {
    const [minipoolAddress, slotsLength] = value.splice(0, 2)
    if (await socketCall(['scores', `${epoch}/${minipoolAddress}`, 'check'])) {
      value.splice(0, parseInt(slotsLength))
      continue
    }
    for (const _ of Array(parseInt(slotsLength)).fill()) {
      const slotIndex = value.shift()
      const blockTime = genesisTime + secondsPerSlot * BigInt(slotIndex)
      const nodeAddress = await cachedCall(minipoolAddress, 'getNodeAddress', [], 'finalized')
      const {optInTime, optOutTime} = await getNodeSmoothingTimes(nodeAddress)
      if (blockTime < optInTime || blockTime > optOutTime) continue
      const statusTime = BigInt(await cachedCall(minipoolAddress, 'getStatusTime', [], 'finalized'))
      if (blockTime < statusTime) continue
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
      if (!minipoolScores.has(minipoolAddress)) minipoolScores.set(minipoolAddress, new Map())
      const scores = minipoolScores.get(minipoolAddress)
      scores.set(slotIndex, minipoolScore)
    }
  }
  return minipoolScores
}

parentPort.on('message', async (msg) => {
  if (msg === 'exit') process.exit()
  const minipoolScores = await processEpoch(msg)
  for (const [minipoolAddress, scores] of minipoolScores)
    await socketCall(['scores', `${msg}/${minipoolAddress}`, Array.from(scores.entries()).join()])
  parentPort.postMessage('done')
})
