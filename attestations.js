import { parentPort, threadId } from 'node:worker_threads'
import { socketCall, slotsPerEpoch, log } from './lib.js'

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
        value.push({minipoolAddress, position})
      })
    }
  }
  if (await socketCall(['duties', prevEpochIndex.toString(), 'check']))
    await addDuties(prevEpochIndex.toString())
  if (await socketCall(['duties', epochIndex.toString(), 'check']))
    await addDuties(epochIndex.toString())

  const minipoolAttestations = new Map()
  const firstSlotToCheck = parseInt(epochIndex * slotsPerEpoch)

  for (const slotToCheck of
    Array.from(Array(parseInt(slotsPerEpoch)).keys()).map(i => firstSlotToCheck + i)) {

    if (!(await socketCall(['beacon', 'checkSlotExists', slotToCheck]))) continue
    const attestations = JSON.parse(await socketCall(['beacon', 'getAttestationsFromSlot', slotToCheck]))

    attestations.forEach(({slotNumber, committeeIndex, attested}) => {
      const slotIndex = parseInt(slotNumber)
      if (slotToCheck <= slotIndex) return
      if (slotToCheck - slotIndex > parseInt(slotsPerEpoch)) return
      const dutyKey = `${slotNumber},${committeeIndex}`
      if (!rocketPoolDuties.has(dutyKey)) return
      const duties = rocketPoolDuties.get(dutyKey)
      duties.forEach(({position, minipoolAddress}) => {
        if (!attested[position]) return
        if (!minipoolAttestations.has(minipoolAddress)) minipoolAttestations.set(minipoolAddress, new Set())
        const slots = minipoolAttestations.get(minipoolAddress)
        slots.add(slotIndex)
      })
    })
  }

  return minipoolAttestations
}

parentPort.on('message', async (msg) => {
  if (msg === 'exit') process.exit()
  const minipoolAttestations = await processSlot(msg)
  const value = []
  minipoolAttestations.forEach((slots, addr) =>
    value.push(addr, slots.size, Array.from(slots.values())))
  await socketCall(['attestations', msg, value.flat().join()])
  parentPort.postMessage('done')
})
