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
// 21
const startEpoch = 269715
const endEpoch = 272456
let epoch = startEpoch

const minipoolScores = new Map()

while (epoch <= endEpoch) {
  console.log(`Importing ${epoch}, ${minipoolScores.size} minipools in map`)
  const minipoolScoresForEpoch = db.get(`/mainnet/scores/${epoch++}`)
  for (const [minipoolAddress, minipoolScore] of minipoolScoresForEpoch.entries()) {
    const score = BigInt(`0x${minipoolScore}`)
    minipoolScores.set(minipoolAddress,
      minipoolScores.has(minipoolAddress) ?
      minipoolScores.get(minipoolAddress) + score :
      score
    )
  }
}

const str = createWriteStream('minipool-scores.json')
const write = s => new Promise(resolve => { if (str.write(s)) { resolve() } else { str.once('drain', resolve) } })
let first = true

for (const [minipoolAddress, minipoolScore] of minipoolScores.entries()) {
  await write(first ? '{\n' : ',\n')
  // console.log(`Writing ${minipoolAddress}`)
  await write(`"${minipoolAddress}":"${minipoolScore}"`)
  first = false
}
str.end('\n}')
