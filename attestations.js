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
  if (await socketCall(['duties', epochIndex.toString(), 'check']))
    await addDuties(epochIndex.toString())

  const minipoolScores = new Map()
  let successfulAttestations = 0n

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
        successfulAttestations++
      })
    })
  }

  return [minipoolScores, successfulAttestations]
}

parentPort.on('message', async (msg) => {
  if (msg === 'exit') process.exit()
  const [minipoolScores, successfulAttestations] = await processSlot(msg)
  const value = [successfulAttestations.toString(), minipoolScores.size.toString()]
  minipoolScores.forEach((score, addr) => value.push(addr, score.toString()))
  await socketCall(['attestations', msg, value.join()])
  parentPort.postMessage('done')
})
