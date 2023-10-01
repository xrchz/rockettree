import { open } from 'lmdb'
const dryRun = false
const dbDir = process.env.DB_DIR || 'db'
const db = open({path: dbDir})

const epoch = 225614
const key = `/mainnet/scores/${epoch}`
const map = db.get(key)
console.log(map.size)
// const {value: [minipoolAddress, slots]} = map.entries().next()
//const minipoolAddress = '0xD279B4a9D04a38Fc8f2C140b2eaB6bd8Ea8fc184'
//const slots = map.get(minipoolAddress)
//console.log(epoch)
//console.log(minipoolAddress)
//console.log(Array.from(slots.values()).join())

/*
const epoch = 225612
const key = `/mainnet/attestations/${epoch}`
console.log(`Deleted ${key} status: ${await db.remove(key)}`)
const key2 = `/mainnet/scores/${epoch}`
console.log(`Deleted ${key2} status: ${await db.remove(key2)}`)
*/

// for (const key of db.getKeys({start: `/mainnet/17769401/nodeSmoothingTimes`, end: '/mainnet/17769401/nodeSmoothingTimes/1'}))
//   console.log(`Deleted ${key} status: ${await db.remove(key)}`)

/*
for (const epoch of Array.from(Array(231904-206715+1).keys()).map(i => 206714 +i)) {
  const key = `/mainnet/attestations/${epoch}`
  console.log(`Deleted ${key} status: ${await db.remove(key)}`)
}
*/

/*
for (const key of db.getKeys({start: `/mainnet//`, end: '/mainnet//\ufff0'}))
   console.log(`Deleted ${key} status: ${await db.remove(key)}`)
*/

/*
console.log(typeof db.get('/mainnet/17769401/0x89F478E6Cc24f052103628f36598D4C14Da3D287/getSmoothingPoolRegistrationState/0x1b00208edE554b42C48988b65614C98FF287Db90'))
process.exit()
*/

/*
console.log(db.get('/mainnet//eth/v1/beacon/states/head/committees?epoch=217204')[2037].validators[291])
process.exit()
*/

// console.log(db.get('/mainnet/eth/v1/beacon/blinded_blocks/6950560/attestations')[49].attested.slice(291-3, 291+3))
// console.log(db.get('/mainnet/eth/v1/beacon/blinded_blocks/6950560/attestations').filter(x => x.slotNumber == 6950559).map(x => `${x.slotNumber},${x.committeeIndex}`))
// process.exit()

// console.log(db.get('/mainnet/17769401/nodeSmoothingTimes/0x1b00208edE554b42C48988b65614C98FF287Db90'))
// process.exit()

// for (const key of db.getKeys({start: `/mainnet/duties/`, end: '/mainnet/duties/\ufff0'}))
//   console.log(key)

// const searchFor = '0xd3F2ee2de4BfcCD4bC667cA01c3a8f4b8ec079Dd'
// skeptical - but true: 0x216b5A5cb93A4d5e2fDbe133321189932dbd57d4 didn't attest 6805920,9 in 6805931
// skeptical: 0x216b5A5cb93A4d5e2fDbe133321189932dbd57d4 opted out 6805920,9
// const searchForMinipool = '0x7D2C4a974E5C5f8E081Fb7890ac31E20d2307Fe7'
// const searchForMinipool = '0x216b5A5cb93A4d5e2fDbe133321189932dbd57d4'
// const searchForMinipool = '0x0087fEFeB538D3100FCDB7E1781f25442C8932E0'
// const searchForNode = '0x02FB6F70942A4037A1E06dDdFAEE4f8581A439D9'
// const result = db.get('/mainnet/17632767/nodeSmoothingTimes/0x02FB6F70942A4037A1E06dDdFAEE4f8581A439D9')
// const result = db.get('/mainnet/17632767/nodeSmoothingTimes/0x02FB6F70942A4037A1E06dDdFAEE4f8581A439D9')
/*
const keys = db.getKeys({start: '/mainnet/17632767/nodeSmoothingTimes/', end: '/mainnet/17632767/nodeSmoothingTimes/1'})
for (const key of keys) {
  if (dryRun) {
    console.log(`Would delete ${key}`)
  }
  else {
    console.log(`Deleted ${key} status: ${await db.remove(key)}`)
  }
}
*/
/*
for (const epoch of Array.from(Array(4500).keys()).map(i => i + 213015)) {
// for (const epoch of Array.from(Array(10).keys()).map(i => i + 208514)) {
  const duties = db.get(`/mainnet/duties/${epoch}`).split(',')
  while (duties.length) {
    const slot = duties.shift()
    const committee = duties.shift()
    let entries = parseInt(duties.shift())
    while (entries--) {
      const minipoolAddress = duties.shift()
      const position = duties.shift()
      if (minipoolAddress == searchForMinipool) {
        // console.log(`Found ${searchFor} duty in ${slot},${committee} @ ${position}`)
        console.log(slot)
      }
    }
  }
}
*/
/*
// const pubkey = '0x82e13070a677ba241bd5ff5422544200e872ee9f9a3d6da39ef0e35e7e552845a80cbc61cb161fbffc58d44f29d52e27'
// const pubkey = '0x90d8ee827e20cc23da85f016191cc240e92cc9b589b272f7741f4ec05a5705d1ccf5aaf354989d5300e1a58476ff0e06'
const pubkey = '0x93f79bc6cf0d3c471af35cfafbc266139919a70eb3df7be98c6ad41977475527067464a35ec04322a0c6963f72724f9a'
// const result = db.getKeys({start: `/mainnet//eth/v1/beacon/states/head/validators/`, end: '/mainnet//eth/v1/beacon/states/head/validators/1'})
// for (const key of result) console.log(key)
const result = db.get(`/mainnet//eth/v1/beacon/states/head/validators/${pubkey}/index`)
console.log(result)
*/
