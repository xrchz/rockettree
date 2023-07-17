import PouchDB from 'pouchdb-node'
const dryRun = true
const dbDir = process.env.DB_DIR || 'db'
const db = new PouchDB(dbDir)
/*
const epoch = '206725'
let prefix = `/mainnet/duties/${epoch}`
let result = await db.allDocs({startkey: prefix, endkey: `${prefix}\ufff0`, include_docs: true})
for (const row of result.rows) {
  if (dryRun)
    console.log(`Would delete ${row.id} ${row.value.rev}`)
  else {
    console.log(`Deleting ${row.id} ${row.value.rev}`)
    await db.remove(row.id, row.value.rev).catch(err =>
      console.error(`Got ${err} when deleting ${row.id} ${row.value.rev}`))
  }
}
prefix = `/mainnet/attestations/${epoch}`
result = await db.allDocs({startkey: prefix, endkey: `${prefix}\ufff0`, include_docs: true})
for (const row of result.rows) {
  if (dryRun)
    console.log(`Would delete ${row.id} ${row.value.rev}`)
  else {
    console.log(`Deleting ${row.id} ${row.value.rev}`)
    await db.remove(row.id, row.value.rev).catch(err =>
      console.error(`Got ${err} when deleting ${row.id} ${row.value.rev}`))
  }
}
*/
// await db.info().then(res => console.log(`Got db info ${JSON.stringify(res)}`))
// await db.compact()
// const prefix = '/mainnet/minipoolAttestations'
const prefix = '/mainnet/attestations'
// const result = await db.allDocs({startkey: prefix, endkey: `${prefix}\ufff0`, include_docs: true})
const result = await db.allDocs({startkey: prefix, endkey: `${prefix}\ufff0`})
for (const row of result.rows) {
  if (dryRun)
    console.log(`Would delete ${row.id} ${row.value.rev}`)
  else {
    console.log(`Deleting ${row.id} ${row.value.rev}`)
    await db.remove(row.id, row.value.rev).catch(err =>
      console.error(`Got ${err} when deleting ${row.id} ${row.value.rev}`))
  }
}
/*
for (const row of result.rows) {
  if (dryRun) {
    console.log(`Would update ${row.id} ${row.value.rev}, with value type ${typeof row.doc.value}`)
  }
  else {
    console.log(`Updating ${row.id} ${row.value.rev}`)
    row.doc.value = row.doc.value.join()
    await db.put(row.doc)
  }
}
*/
/*
const prefix = '/mainnet/duties'
const result = await db.allDocs({startkey: prefix, endkey: `${prefix}\ufff0`, include_docs: true})
console.log(`total_rows: ${result.total_rows}`)
console.log(`rows.length: ${result.rows.length}`)
for (const row of result.rows) {
  if (dryRun)
    console.log(`Would update ${row.id} ${row.value.rev}`)
  else {
    console.log(`Updating ${row.id} ${row.value.rev}`)
    const oldValue = row.doc.value.split(',')
    const newValue = []
    let currentKey
    let lengthIndex
    while (oldValue.length) {
      const [slotIndex, committeeIndex, minipoolAddress, position, minipoolScore] = oldValue.splice(0, 5)
      const dutyKey = `${slotIndex},${committeeIndex}`
      if (dutyKey != currentKey) {
        newValue.push(slotIndex, committeeIndex)
        currentKey = dutyKey
        lengthIndex = newValue.length
        newValue.push(0)
      }
      newValue[lengthIndex]++
      newValue.push(minipoolAddress, position, minipoolScore)
    }
    row.doc.value = newValue.join()
    await db.put(row.doc)
  }
}
*/
/*
const prefix = '/mainnet/finalized'
const result = await db.allDocs({startkey: prefix, endkey: `${prefix}\ufff0`})
console.log(`total_rows: ${result.total_rows}`)
console.log(`rows.length: ${result.rows.length}`)
for (const row of result.rows) {
  if (!row.id.includes('getNodeFee'))
    continue
  if (dryRun)
    console.log(`Would delete ${row.id} ${row.value.rev}`)
  else {
    console.log(`Deleting ${row.id} ${row.value.rev}`)
    await db.remove(row.id, row.value.rev).catch(err =>
      console.error(`Got ${err} when deleting ${row.id} ${row.value.rev}`))
  }
}
*/
/*
for (const row of result.rows) {
  if (row.id.endsWith('/blockNumber') || row.id.endsWith('/attestations')) continue
  if (dryRun)
    console.log(`Would delete ${row.id} ${row.value.rev}`)
  else {
    console.log(`Deleting ${row.id} ${row.value.rev}`)
    await db.remove(row.id, row.value.rev).catch(err =>
      console.error(`Got ${err} when deleting ${row.id} ${row.value.rev}`))
  }
}
*/
// const doc = await db.get('/mainnet/17519633/nodeSmoothingTimes/0x33043c521E9c3e80E0c05A2c25f2e894FefC0328')
// const doc = await db.get('/mainnet/finalized/0xf7aB34C74c02407ed653Ac9128731947187575C0/getLastBondReductionPrevNodeFee/0x1732EC77fdD9B0f571F05F01d3CB0aee66fdcB00')
// const doc = await db.get('/mainnet/attestations/206715')
//for (const epoch of Array.from(Array(4000).keys()).map(i => 210700 + i)) {
//  const doc = await db.get(`/mainnet/duties/${epoch}`)
//  const duties = doc.value.split(',')
//  if (duties.length >= 5 && parseInt(duties[2]) > 0 && BigInt(duties[5]) >= BigInt(3200e18)) {
//    console.log(`Would delete ${epoch} because there is a large score ${duties[5]}`)
//    /*
//    await db.remove(doc._id, doc._rev).catch(err =>
//        console.error(`Got ${err} when deleting ${doc._id} ${doc._rev}`))
//   */
//  }
//  else console.log(`${epoch} fine`)
//}
// console.log(`Deleting ${doc._id} ${doc._rev}`)
// await db.remove(doc._id, doc._rev).catch(err =>
//     console.error(`Got ${err} when deleting ${doc._id} ${doc._rev}`))
// 0x05E3DD28852195a41ac724CfD646CB4E4fDB5D20
// const doc = await db.get('/mainnet/finalized/0x9C368fe662E603cCBFDFAa02B654D96cD929508B/getNodeFee/')
// const doc = await db.get('/mainnet/finalized/0x9C368fe662E603cCBFDFAa02B654D96cD929508B/getNodeDepositBalance/')
// const doc = await db.get('/mainnet/attestations/207819')
/*
const doc = await db.get('/mainnet/attestations/206715')
const atts = doc.value.split(',')
const addr = '0xc41dd1311f99E55c1A2963416d93279d70D4220f'
while (atts.length) {
  if (atts[0] == addr) {
    console.log(atts.shift())
    const n = parseInt(atts.shift())
    Array(n).fill().forEach(() => {
      console.log(atts.shift())
      console.log(atts.shift())
    })
  }
  else {
    atts.shift()
    const n = parseInt(atts.shift())
    Array(n).fill().forEach(() => atts.splice(0, 2))
  }
}
*/

await db.close()
