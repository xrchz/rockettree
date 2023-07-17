import { socketCall, slotsPerEpoch } from './lib.js'
import { openSync, writeSync, closeSync } from 'node:fs'

const targetSlotEpoch = BigInt(await socketCall(['targetSlotEpoch']))
const ConsensusBlock = BigInt(await socketCall(['ConsensusBlock']))
const bnStartEpoch = ConsensusBlock / slotsPerEpoch + 1n

const successes = new Map()

const epochs = Array.from(
  Array(parseInt(targetSlotEpoch + 1n - bnStartEpoch + 1n)).keys())
.map(i => bnStartEpoch + BigInt(i))
while (epochs.length) {
  if (epochs.length % 10 == 0)
    console.log(`${epochs.length} epochs left`)
  const epoch = epochs.shift().toString()
  const array = await socketCall(['minipoolAttestations', epoch]).then(s => s.split(','))
  while (array.length) {
    const [minipoolAddress, slotsLength] = array.splice(0, 2)
    if (!successes.has(minipoolAddress)) successes.set(minipoolAddress, [])
    const slots = successes.get(minipoolAddress)
    Array(parseInt(slotsLength)).fill().forEach(() => slots.push(array.shift()))
  }
}

const fd = openSync('minipoolSuccesses.txt', 'w')
successes.forEach((slots, addr) => writeSync(fd, `"${addr}": [${slots}],\n`))
closeSync(fd)
