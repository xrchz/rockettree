import 'dotenv/config'
import { ethers } from 'ethers'
import PouchDB from 'pouchdb-node'

const dbDir = process.env.DB_DIR || 'db'
const db = new PouchDB(dbDir)

const rocketStorageAddresses = new Map()
rocketStorageAddresses.set('mainnet', '0x1d8f8f00cfa6758d7bE78336684788Fb0ee0Fa46')
rocketStorageAddresses.set('goerli', '0xd8Cd47263414aFEca62d6e2a3917d6600abDceB3')

const genesisTimes = new Map()
genesisTimes.set('mainnet', 1606824023n)
genesisTimes.set('goerli', 1616508000n)

const beaconRpcUrl = process.env.BN_URL || 'http://localhost:5052'
const provider = new ethers.JsonRpcProvider(process.env.RPC_URL || 'http://localhost:8545')
const networkName = await provider.getNetwork().then(n => n.name)
const rocketStorage = new ethers.Contract(
  rocketStorageAddresses.get(networkName),
  ['function getAddress(bytes32) view returns (address)'], provider)
console.log(`Using Rocket Storage ${await rocketStorage.getAddress()}`)

const bigIntPrefix = 'BI:'
const numberPrefix = 'N:'
function serialise(result) {
  const type = typeof result
  if (type === 'bigint')
    return bigIntPrefix.concat(result.toString(16))
  else if (type === 'number')
    return numberPrefix.concat(result.toString(16))
  else if (type === 'string' || type === 'object' || type === 'boolean')
    return result
  else {
    throw new Error(`serialise unhandled type: ${type}`)
  }
}
function deserialise(data) {
  if (typeof data !== 'string')
    return data
  else if (data.startsWith(bigIntPrefix))
    return BigInt(`0x${data.substring(bigIntPrefix.length)}`)
  else if (data.startsWith(numberPrefix))
    return parseInt(`0x${data.substring(numberPrefix.length)}`)
  else
    return data
}

async function cachedCall(contract, fn, args, blockTag) {
  const address = await contract.getAddress()
  const key = `/${networkName}/${blockTag.toString()}/${address}/${fn}/${args.map(a => a.toString()).join()}`
  return await db.get(key)
    .then(
      doc => deserialise(doc.value),
      async err => {
        const result = await contract[fn](...args, {blockTag})
        await db.put({_id: key, value: serialise(result)})
        return result
      }
    )
}

async function cachedBeacon(path, result) {
  const key = `/${networkName}/${path}`
  if (result === undefined) {
    return await db.get(key).then(
      doc => deserialise(doc.value),
      err => result
    )
  }
  else await db.put({_id: key, value: serialise(result)})
}

const getRocketAddress = (name, blockTag) =>
  cachedCall(rocketStorage, 'getAddress(bytes32)', [ethers.id(`contract.address${name}`)], blockTag)

const startBlock = parseInt(process.env.START_BLOCK) || 'latest'

const rocketRewardsPool = new ethers.Contract(
  await getRocketAddress('rocketRewardsPool', startBlock),
  ['function getClaimIntervalTimeStart() view returns (uint256)',
   'function getClaimIntervalTime() view returns (uint256)',
   'function getPendingRPLRewards() view returns (uint256)',
   'function getClaimingContractPerc(string) view returns (uint256)'
  ],
  provider)

const startTime = await cachedCall(rocketRewardsPool, 'getClaimIntervalTimeStart', [], startBlock)
console.log(`startTime: ${startTime}`)

const intervalTime = await cachedCall(rocketRewardsPool, 'getClaimIntervalTime', [], startBlock)
console.log(`intervalTime: ${intervalTime}`)

const latestBlockTime = await provider.getBlock('latest').then(b => b.timestamp)
console.log(`latestBlockTime: ${latestBlockTime}`)

const timeSinceStart = BigInt(latestBlockTime) - BigInt(startTime)
const intervalsPassed = timeSinceStart / intervalTime
console.log(`intervalsPassed: ${intervalsPassed}`)

const genesisTime = genesisTimes.get(networkName)
const secondsPerSlot = 12n
const slotsPerEpoch = 32n

const endTime = startTime + (intervalTime * intervalsPassed)
console.log(`endTime: ${endTime}`)

const totalTimespan = endTime - genesisTime
console.log(`totalTimespan: ${totalTimespan}`)

let targetBcSlot = totalTimespan / secondsPerSlot
if (totalTimespan % secondsPerSlot) targetBcSlot++

const targetSlotEpoch = targetBcSlot / slotsPerEpoch
console.log(`targetSlotEpoch: ${targetSlotEpoch}`)
targetBcSlot = (targetSlotEpoch + 1n) * slotsPerEpoch - 1n
console.log(`last (possibly missing) slot in epoch: ${targetBcSlot}`)

async function checkSlotExists(slotNumber) {
  const path = `/eth/v1/beacon/headers/${slotNumber}`
  const cache = await cachedBeacon(path); if (cache !== undefined) return cache
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200 && response.status !== 404)
    console.warn(`Unexpected response status getting ${slotNumber} header: ${response.status}`)
  const result = response.status === 200
  await cachedBeacon(path, result); return result
}
while (!(await checkSlotExists(targetBcSlot))) targetBcSlot--

console.log(`targetBcSlot: ${targetBcSlot}`)

async function getBlockNumberFromSlot(slotNumber) {
  const path = `/eth/v1/beacon/blocks/${slotNumber}`
  const cache = await cachedBeacon(path); if (cache !== undefined) return cache
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200)
    console.warn(`Unexpected response status getting ${slotNumber} block: ${response.status}`)
  const json = await response.json()
  const result = BigInt(json.data.message.body.execution_payload.block_number)
  await cachedBeacon(path, result); return result
}

async function getValidatorStatus(slotNumber, pubkey) {
  const path = `/eth/v1/beacon/states/${slotNumber}/validators/${pubkey}`
  const cache = await cachedBeacon(path); if (cache !== undefined) return cache
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200)
    console.warn(`Unexpected response status getting ${pubkey} state at ${slotNumber}: ${response.status}`)
  const result = await response.json().then(j => j.data.validator)
  await cachedBeacon(path, result); return result
}

const targetElBlock = await getBlockNumberFromSlot(targetBcSlot)
console.log(`targetElBlock: ${targetElBlock}`)
const targetElBlockTimestamp = await provider.getBlock(targetElBlock).then(b => b.timestamp)
console.log(`targetElBlockTimestamp: ${targetElBlockTimestamp}`)

const pendingRewards = await cachedCall(
  rocketRewardsPool, 'getPendingRPLRewards', [], targetElBlock)
const collateralPercent = await cachedCall(
  rocketRewardsPool, 'getClaimingContractPerc', ['rocketClaimNode'], targetElBlock)
const oDaoPercent = await cachedCall(
  rocketRewardsPool, 'getClaimingContractPerc', ['rocketClaimTrustedNode'], targetElBlock)
const pDaoPercent = await cachedCall(
  rocketRewardsPool, 'getClaimingContractPerc', ['rocketClaimDAO'], targetElBlock)

const _100Percent = ethers.parseEther('1')
const collateralRewards = pendingRewards * collateralPercent / _100Percent
const oDaoRewards = pendingRewards * oDaoPercent / _100Percent
const pDaoRewards = pendingRewards * pDaoPercent / _100Percent
console.log(`pendingRewards: ${pendingRewards}`)
console.log(`collateralRewards: ${collateralRewards}`)
console.log(`oDaoRewards: ${oDaoRewards}`)
console.log(`pDaoRewards: ${pDaoRewards}`)
