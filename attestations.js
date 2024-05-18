import { parentPort, workerData, threadId } from 'node:worker_threads'
import { socketCall, denebEpoch, slotsPerEpoch, genesisTime, secondsPerSlot, log } from './lib.js'

const currentIndex = BigInt(await socketCall(['elState', 'rocketRewardsPool', 'getRewardIndex']))

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

async function processEpoch(epochToCheck) {
  const rocketPoolDuties = new Map()
  const epochIndex = BigInt(epochToCheck)
  const prevEpochIndex = epochIndex - 1n
  const addDuties = async (epoch) => {
    const duties = await socketCall(['duties', epoch]).then(s => s.split(','))
    while (duties.length) {
      const dutyKey = duties.splice(0, 2).join()
      const value = []
      rocketPoolDuties.set(dutyKey, value)
      Array(parseInt(duties.shift())).fill().forEach(() => {
        const minipoolAddress = duties.shift()
        const position = parseInt(duties.shift())
        value.push({minipoolAddress, position})
      })
    }
  }
  if (workerData.value.bnStartEpoch <= prevEpochIndex)
    await addDuties(prevEpochIndex.toString())
  if (epochIndex <= workerData.value.targetSlotEpoch)
    await addDuties(epochIndex.toString())

  const firstSlotToCheck = parseInt(epochIndex * slotsPerEpoch)

  for (const slotToCheck of
    Array.from(Array(parseInt(slotsPerEpoch)).keys()).map(i => firstSlotToCheck + i)) {

    if (!(await socketCall(['beacon', 'checkSlotExists', slotToCheck]))) continue
    const attestations = await socketCall(['beacon', 'getAttestationsFromSlot', slotToCheck])

    // TODO: try as Promise.all?
    for (const {slotNumber, committeeIndex, attested} of attestations) {
      const slotIndex = parseInt(slotNumber)
      if (slotToCheck <= slotIndex) continue
      const epoch = parseInt(BigInt(slotIndex) / slotsPerEpoch)
      if (epoch < denebEpoch)
        if (slotToCheck - slotIndex > parseInt(slotsPerEpoch)) continue
      else
        if (BigInt(slotToCheck) / slotsPerEpoch > epoch + 1) continue
      const dutyKey = `${slotNumber},${committeeIndex}`
      if (!rocketPoolDuties.has(dutyKey)) continue
      const blockTime = genesisTime + secondsPerSlot * BigInt(slotIndex)
      // TODO: try as Promise.all?
      for (const {position, minipoolAddress} of rocketPoolDuties.get(dutyKey)) {
        if (!attested[position]) continue
        const nodeAddress = await socketCall(['elState', minipoolAddress, 'getNodeAddress'])
        const {optInTime, optOutTime} = await getNodeSmoothingTimes(nodeAddress)
        if (blockTime < optInTime || blockTime > optOutTime) continue
        const statusTime = BigInt(await socketCall(['elState', minipoolAddress, 'getStatusTime']))
        if (blockTime < statusTime) continue
        parentPort.postMessage({minipoolAddress, slotIndex})
      }
    }
  }
}

parentPort.on('message', async (msg) => {
  await processEpoch(msg)
  parentPort.postMessage({minipoolAddress: msg, slotIndex: 'done'})
})
