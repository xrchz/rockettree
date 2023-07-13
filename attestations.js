import { parentPort, workerData, threadId } from 'node:worker_threads'
import { socketCall, slotsPerEpoch, addressToUint64s, uint256To64s, log } from './lib.js'

async function processSlot(epochToCheck) {
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
        const minipoolScore = BigInt(duties.shift())
        value.push({minipoolAddress, position, minipoolScore})
      })
    }
  }
  if (await socketCall(['duties', prevEpochIndex.toString(), 'check']))
    await addDuties(prevEpochIndex.toString())
  await addDuties(epochIndex.toString())

  const minipoolScores = new Map()

  const firstSlotToCheck = parseInt(epochIndex * slotsPerEpoch)
  for (const slotToCheck of
    Array.from(Array(parseInt(slotsPerEpoch)).keys()).map(i => firstSlotToCheck + i)) {

    if (!(await socketCall(['beacon', 'checkSlotExists', slotToCheck]))) continue
    const attestations = JSON.parse(await socketCall(['beacon', 'getAttestationsFromSlot', slotToCheck]))

    attestations.forEach(({slotNumber, committeeIndex, attested}) => {
      if (slotToCheck <= parseInt(slotNumber)) return
      if (slotToCheck - parseInt(slotNumber) > parseInt(slotsPerEpoch)) return
      const dutyKey = `${slotNumber},${committeeIndex}`
      if (!rocketPoolDuties.has(dutyKey)) return
      const duties = rocketPoolDuties.get(dutyKey)
      duties.forEach(({position, minipoolAddress, minipoolScore}) => {
        if (!attested[position]) return
        const oldScore = minipoolScores.has(minipoolAddress) ?
                         minipoolScores.get(minipoolAddress) : 0n
        minipoolScores.set(minipoolAddress, oldScore + minipoolScore)
      })
    })
  }

  return minipoolScores
}

parentPort.on('message', async (msg) => {
  if (msg === 'exit') process.exit()
  const minipoolScores = await processSlot(msg)
  const returnData = new BigUint64Array(minipoolScores.size * 7)
  let i = 0
  minipoolScores.forEach((score, addr) => {
    returnData.set(addressToUint64s(addr), i); i += 3
    returnData.set(uint256To64s(score), i); i += 4
  })
  parentPort.postMessage(returnData, [returnData.buffer])
})
