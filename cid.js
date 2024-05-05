import { importer } from 'ipfs-unixfs-importer'
import { MemoryBlockstore } from 'blockstore-core'
import { readFileSync } from 'node:fs'

const sources = [{
  path: process.env.FILENAME.split('/').at(-1),
  content: readFileSync(process.env.FILENAME)
}]
const options = {
  wrapWithDirectory: true,
  reduceSingleLeafToSelf: true,
  shardSplitThresholdBytes: 4 * 1024 * 1024,
  rawLeaves: true,
  cidVersion: 1
}
const blockstore = new MemoryBlockstore()
for await (const {cid, path} of importer(sources, blockstore, options))
  if (!path) console.log(cid.toString())
