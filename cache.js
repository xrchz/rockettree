import 'dotenv/config'
import PouchDB from 'pouchdb-node'
import { ethers } from 'ethers'
import { createServer } from 'node:net'
import { log, tryBigInt, provider, startBlock, genesisTime,
         secondsPerSlot, slotsPerEpoch, networkName, socketPath } from './lib.js'

const dbDir = process.env.DB_DIR || 'db'
const db = new PouchDB(dbDir)

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

const beaconRpcUrl = process.env.BN_URL || 'http://localhost:5052'

function hexStringToBitlist(s) {
  const bitlist = []
  let hexDigits = s.substring(2)
  if (hexDigits.length % 2 !== 0)
    hexDigits = `0${hexDigits}`
  let i
  while (hexDigits.length) {
    const byteStr = hexDigits.substring(0, 2)
    hexDigits = hexDigits.substring(2)
    const uint8 = parseInt(`0x${byteStr}`)
    i = 1
    while (i < 256) {
      bitlist.push(!!(uint8 & i))
      i *= 2
    }
  }
  i = bitlist.length
  while (!bitlist[--i])
    bitlist.pop()
  bitlist.pop()
  return bitlist
}

async function getAttestationsFromSlot(slotNumber) {
  const path = `/eth/v1/beacon/blinded_blocks/${slotNumber}`
  const key = `${path}/attestations`
  const cache = await cachedBeacon(key); if (cache !== undefined) return cache
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200)
    console.warn(`Unexpected response status getting ${slotNumber} attestations: ${response.status}`)
  const json = await response.json()
  const result = json.data.message.body.attestations.map(
    ({aggregation_bits, data: {slot, index}}) =>
    ({attested: hexStringToBitlist(aggregation_bits),
      slotNumber: slot, committeeIndex: index})
  )
  await cachedBeacon(key, result); return result
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

async function getIndexFromPubkey(pubkey) {
  const path = `/eth/v1/beacon/states/head/validators/${pubkey}`
  const key = `${path}/index`
  const cache = await cachedBeacon(key); if (cache !== undefined) return cache
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200)
    console.warn(`Unexpected response status getting ${pubkey} index: ${response.status}`)
  const result = await response.json().then(j => j.data.index)
  await cachedBeacon(key, result); return result
}

async function getCommittees(epochIndex) {
  const path = `/eth/v1/beacon/states/head/committees?epoch=${epochIndex}`
  const cache = await cachedBeacon(path); if (cache !== undefined) return cache
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200)
    console.warn(`Unexpected response status getting epoch ${epochIndex}: ${response.status}`)
  const result = await response.json().then(j => j.data)
  await cachedBeacon(path, result); return result
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

async function getBlockNumberFromSlot(slotNumber) {
  const path = `/eth/v1/beacon/blinded_blocks/${slotNumber}`
  const key = `${path}/blockNumber`
  const cache = await cachedBeacon(key); if (cache !== undefined) return cache
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200) {
    console.warn(`Unexpected response status getting ${slotNumber} block: ${response.status}`)
    console.warn(`response text: ${await response.text()}`)
  }
  const json = await response.json()
  const result = BigInt(json.data.message.body.execution_payload_header.block_number)
  await cachedBeacon(key, result); return result
}

const getMinipool = (addr) =>
  new ethers.Contract(addr,
    ['function getStatus() view returns (uint8)',
     'function getStatusTime() view returns (uint256)',
     'function getUserDepositBalance() view returns (uint256)',
     'function getNodeDepositBalance() view returns (uint256)',
     'function getNodeFee() view returns (uint256)'
    ],
    provider)

const contracts = new Map()
const getContract = (name) => {
  if (contracts.has(name))
    return contracts.get(name)
  const minipool = getMinipool(name)
  contracts.set(name, minipool)
  return minipool
}

const rocketStorageAddresses = new Map()
rocketStorageAddresses.set('mainnet', '0x1d8f8f00cfa6758d7bE78336684788Fb0ee0Fa46')
rocketStorageAddresses.set('goerli', '0xd8Cd47263414aFEca62d6e2a3917d6600abDceB3')

const rocketStorage = new ethers.Contract(
  rocketStorageAddresses.get(networkName),
  ['function getAddress(bytes32) view returns (address)'], provider)
log(2, `Using Rocket Storage ${await rocketStorage.getAddress()}`)
contracts.set('rocketStorage', rocketStorage)

const getRocketAddress = (name, blockTag) =>
  cachedCall(rocketStorage, 'getAddress(bytes32)', [ethers.id(`contract.address${name}`)], blockTag)

const rocketRewardsPool = new ethers.Contract(
  await getRocketAddress('rocketRewardsPool', startBlock),
  ['function getClaimIntervalTimeStart() view returns (uint256)',
   'function getClaimIntervalTime() view returns (uint256)',
   'function getPendingRPLRewards() view returns (uint256)',
   'function getClaimingContractPerc(string) view returns (uint256)',
   'function getRewardIndex() view returns (uint256)',
   // struct RewardSubmission {uint256 rewardIndex; uint256 executionBlock; uint256 consensusBlock; bytes32 merkleRoot; string merkleTreeCID; uint256 intervalsPassed; uint256 treasuryRPL; uint256[] trustedNodeRPL; uint256[] nodeRPL; uint256[] nodeETH; uint256 userETH;}
   'event RewardSnapshot(uint256 indexed rewardIndex, '+
    '(uint256, uint256, uint256, bytes32, string,' +
    ' uint256, uint256, uint256[], uint256[], uint256[], uint256) submission, ' +
    'uint256 intervalStartTime, uint256 intervalEndTime, uint256 time)'
  ],
  provider)
contracts.set('rocketRewardsPool', rocketRewardsPool)

const startTime = await cachedCall(rocketRewardsPool, 'getClaimIntervalTimeStart', [], startBlock)
log(1, `startTime: ${startTime}`)

const intervalTime = await cachedCall(rocketRewardsPool, 'getClaimIntervalTime', [], startBlock)
log(2, `intervalTime: ${intervalTime}`)

const latestBlockTime = await provider.getBlock('latest').then(b => BigInt(b.timestamp))
log(2, `latestBlockTime: ${latestBlockTime}`)

const timeSinceStart = latestBlockTime - startTime
const intervalsPassed = timeSinceStart / intervalTime
log(2, `intervalsPassed: ${intervalsPassed}`)

const endTime = startTime + (intervalTime * intervalsPassed)
log(1, `endTime: ${endTime}`)

const totalTimespan = endTime - genesisTime
log(2, `totalTimespan: ${totalTimespan}`)

let targetBcSlot = totalTimespan / secondsPerSlot
if (totalTimespan % secondsPerSlot) targetBcSlot++

const targetSlotEpoch = tryBigInt(process.env.OVERRIDE_TARGET_EPOCH) || targetBcSlot / slotsPerEpoch
log(1, `targetSlotEpoch: ${targetSlotEpoch}`)
targetBcSlot = (targetSlotEpoch + 1n) * slotsPerEpoch - 1n
log(2, `last (possibly missing) slot in epoch: ${targetBcSlot}`)

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

log(1, `targetBcSlot: ${targetBcSlot}`)

const targetElBlock = await getBlockNumberFromSlot(targetBcSlot)
log(1, `targetElBlock: ${targetElBlock}`)

const targetElBlockTimestamp = await provider.getBlock(targetElBlock).then(b => BigInt(b.timestamp))
log(2, `targetElBlockTimestamp: ${targetElBlockTimestamp}`)

const rocketNodeManager = new ethers.Contract(
  await getRocketAddress('rocketNodeManager', targetElBlock),
  ['function getNodeCount() view returns (uint256)',
   'function getNodeAt(uint256) view returns (address)',
   'function getNodeRegistrationTime(address) view returns (uint256)',
   'function getSmoothingPoolRegistrationState(address) view returns (bool)',
   'function getSmoothingPoolRegistrationChanged(address) view returns (uint256)'
  ],
  provider)
contracts.set('rocketNodeManager', rocketNodeManager)

const rocketNodeStaking = new ethers.Contract(
  await getRocketAddress('rocketNodeStaking', targetElBlock),
  ['function getNodeRPLStake(address) view returns (uint256)'],
  provider)
contracts.set('rocketNodeStaking', rocketNodeStaking)

const rocketMinipoolManager = new ethers.Contract(
  await getRocketAddress('rocketMinipoolManager', targetElBlock),
  ['function getNodeMinipoolAt(address, uint256) view returns (address)',
   'function getNodeMinipoolCount(address) view returns (uint256)',
   'function getMinipoolPubkey(address) view returns (bytes)',
   'function getMinipoolCount() view returns (uint256)'
  ],
  provider)
contracts.set('rocketMinipoolManager', rocketMinipoolManager)

const rocketNetworkPrices = new ethers.Contract(
  await getRocketAddress('rocketNetworkPrices', targetElBlock),
  ['function getRPLPrice() view returns (uint256)'],
  provider)
contracts.set('rocketNetworkPrices', rocketNetworkPrices)

const rocketDAOProtocolSettingsNode = new ethers.Contract(
  await getRocketAddress('rocketDAOProtocolSettingsNode', targetElBlock),
  ['function getMinimumPerMinipoolStake() view returns (uint256)',
   'function getMaximumPerMinipoolStake() view returns (uint256)'
  ],
  provider)
contracts.set('rocketDAOProtocolSettingsNode', rocketDAOProtocolSettingsNode)

const rocketDAONodeTrusted = new ethers.Contract(
  await getRocketAddress('rocketDAONodeTrusted', targetElBlock),
  ['function getMemberCount() view returns (uint256)',
   'function getMemberAt(uint256) view returns (address)',
   'function getMemberJoinedTime(address) view returns (uint256)'
  ],
  provider)
contracts.set('rocketDAONodeTrusted', rocketDAONodeTrusted)

const rocketNetworkPenalties = new ethers.Contract(
  await getRocketAddress('rocketNetworkPenalties', targetElBlock),
  ['function getPenaltyCount(address) view returns (uint256)'],
  provider)
contracts.set('rocketNetworkPenalties', rocketNetworkPenalties)

const rocketMinipoolBondReducer = new ethers.Contract(
  await getRocketAddress('rocketMinipoolBondReducer', targetElBlock),
  ['function getLastBondReductionPrevValue(address) view returns (uint256)',
   'function getLastBondReductionPrevNodeFee(address) view returns (uint256)',
   'function getLastBondReductionTime(address) view returns (uint256)'],
  provider)
contracts.set('rocketMinipoolBondReducer', rocketMinipoolBondReducer)

const server = createServer({allowHalfOpen: true}, socket => {
  socket.setEncoding('utf8')
  const data = []
  socket.on('data', (chunk) => data.push(chunk))
  socket.on('end', async () => {
    const request = data.join('')
    const splits = request.split('/')
    if (splits.length == 5 && splits[0] == 'contract') {
      const [contractName, fn, argsJoined, blockTagName] = splits.slice(1)
      const args = argsJoined.split(',')
      const contract = getContract(contractName)
      const blockTag = blockTagName == 'targetElBlock' ? targetElBlock : blockTagName
      const result = await cachedCall(contract, fn, args, blockTag)
      socket.end(result.toString())
    }
    else if (splits.length >= 3 && splits[0] == 'beacon') {
      if (splits[1] == 'getAttestationsFromSlot' && splits.length == 3)
        socket.end(JSON.stringify(await getAttestationsFromSlot(splits[2])))
      else if (splits[1] == 'getValidatorStatus' && splits.length == 4)
        socket.end(JSON.stringify(await getValidatorStatus(splits[2], splits[3])))
      else if (splits[1] == 'getIndexFromPubkey' && splits.length == 3)
        socket.end((await getIndexFromPubkey(splits[2])).toString())
      else if (splits[1] == 'getCommittees' && splits.length == 3)
        socket.end(JSON.stringify(await getCommittees(splits[2])))
      else
        socket.end(`invalid request: unknown beacon request ${splits[1]}`)
    }
    else
      socket.end('invalid request')
  })
})
server.listen(socketPath)