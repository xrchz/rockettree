import { createWriteStream } from 'node:fs'
import { open } from 'lmdb'

const dbDir = 'db'
const db = open({path: dbDir})

// 12
// const startEpoch = 206715
// const endEpoch = 213014
// 13
// const startEpoch = 219315
// const endEpoch = 225614
// 15 partial
const startEpoch = 231915
const endEpoch = 234842
let epoch = startEpoch

const minipoolAttestations = new Map()

while (epoch <= endEpoch) {
  // console.log(`Importing ${epoch}`)
  const minipoolAttestationsForEpoch = db.get(`/mainnet/attestations/${epoch++}`)
  for (const [minipoolAddress, slots] of minipoolAttestationsForEpoch.entries()) {
    if (!minipoolAttestations.has(minipoolAddress))
      minipoolAttestations.set(minipoolAddress, new Set())
    const s = minipoolAttestations.get(minipoolAddress)
    slots.forEach(n => s.add(n))
  }
}

const str = createWriteStream('minipool-attestation-slots.json')
const write = s => new Promise(resolve => { if (str.write(s)) { resolve() } else { str.once('drain', resolve) } })
let first = true

for (const [minipoolAddress, slots] of minipoolAttestations.entries()) {
  await write(first ? '{\n' : ',\n')
  // console.log(`Writing ${minipoolAddress}`)
  await write(`"${minipoolAddress}":`)
  first = true
  for (const slot of slots.keys()) {
    await write(first ? '[' : ',')
    first = false
    await write(`${slot}`)
  }
  first = false
  await write(']')
}
str.end('\n}')
