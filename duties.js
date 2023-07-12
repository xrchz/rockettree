import { ethers } from 'ethers'
import { uint64sToAddress, addressToUint64s, uint256To64s, genesisTime, secondsPerSlot,
         cachedCall, socketCall, log } from './lib.js'
import { parentPort, workerData, threadId } from 'node:worker_threads'

const possiblyEligibleMinipools = new Map()
let i = 0
const possiblyEligibleMinipoolIndices = workerData[0]
while (i < possiblyEligibleMinipoolIndices) {
  const [index, nodeAddress0, nodeAddress1, nodeAddress2,
         minipoolAddress0, minipoolAddress1, minipoolAddress2] =
    workerData.slice(1 + (1 + 3 + 3) * i, 1 + (1 + 3 + 3) * ++i)
  possiblyEligibleMinipools.set(parseInt(index), {
    nodeAddress: uint64sToAddress([nodeAddress0, nodeAddress1, nodeAddress2]),
    minipoolAddress: uint64sToAddress([minipoolAddress0, minipoolAddress1, minipoolAddress2])
  })
}

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

async function processCommittees(epochIndex) {
  const duties = []
  const committees = JSON.parse(await socketCall(['beacon', 'getCommittees', epochIndex]))
  log(3, `${threadId} processing ${committees.length} committees for ${epochIndex}`)
  for (const committee of committees) {
    const slotIndex = BigInt(committee.slot)
    const committeeIndex = BigInt(committee.index)
    const blockTime = genesisTime + secondsPerSlot * slotIndex
    duties.push(slotIndex.toString(), committeeIndex.toString())
    const lengthIndex = duties.length
    duties.push(0)
    for (const [position, validatorIndexStr] of committee.validators.entries()) {
      const validatorIndex = parseInt(validatorIndexStr)
      if (!possiblyEligibleMinipools.has(validatorIndex)) continue
      const {nodeAddress, minipoolAddress} = possiblyEligibleMinipools.get(validatorIndex)
      const {optInTime, optOutTime} = await getNodeSmoothingTimes(nodeAddress)
      if (blockTime < optInTime || blockTime > optOutTime) continue
      const statusTime = BigInt(await cachedCall(minipoolAddress, 'getStatusTime', [], 'finalized'))
      if (blockTime < statusTime) continue
      const currentBond = BigInt(await cachedCall(minipoolAddress, 'getNodeDepositBalance', [], 'finalized'))
      const currentFee = BigInt(await cachedCall(minipoolAddress, 'getNodeFee', [], 'finalized'))
      const previousBond = BigInt(await cachedCall(
        'rocketMinipoolBondReducer', 'getLastBondReductionPrevValue', [minipoolAddress], 'finalized'))
      const previousFee = BigInt(await cachedCall(
        'rocketMinipoolBondReducer', 'getLastBondReductionPrevNodeFee', [minipoolAddress], 'finalized'))
      const lastReduceTime = BigInt(await cachedCall(
        'rocketMinipoolBondReducer', 'getLastBondReductionTime', [minipoolAddress], 'finalized'))
      const {bond, fee} = lastReduceTime > 0 && lastReduceTime > blockTime ?
                          {bond: previousBond, fee: previousFee} :
                          {bond: currentBond, fee: currentFee}
      const minipoolScore = (BigInt(1e18) - fee) * bond / BigInt(32e18) + fee
      duties[lengthIndex]++
      duties.push(minipoolAddress, position.toString(), minipoolScore.toString())
    }
    if (duties[lengthIndex])
      duties[lengthIndex] = duties[lengthIndex].toString()
    else
      duties.splice(-3, 3)
  }
  await socketCall(['duties', epochIndex, duties.join()])
}

parentPort.on('message', async (msg) => {
  if (msg === 'exit') process.exit()
  await processCommittees(msg)
  parentPort.postMessage('done')
})
