import PouchDB from 'pouchdb-node'
const dryRun = true
const dbDir = process.env.DB_DIR || 'db'
const db = new PouchDB(dbDir)
// await db.info().then(res => console.log(`Got db info ${JSON.stringify(res)}`))
// process.exit()
// await db.compact()
const prefix = '/mainnet//eth/v1/beacon'
const result = await db.allDocs({startkey: prefix, endkey: `${prefix}\ufff0`})
console.log(`total_rows: ${result.total_rows}`)
console.log(`rows.length: ${result.rows.length}`)
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
await db.close()
