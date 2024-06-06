import 'dotenv/config'
import { open } from 'lmdb'
import { workerData, parentPort } from 'node:worker_threads'
import { ethers } from 'ethers'
import { log, tryBigInt, makeLock, provider, startBlock, genesisTime,
         secondsPerSlot, slotsPerEpoch, networkName } from './lib.js'
import { MulticallProvider } from "@ethers-ext/provider-multicall"

const dbDir = process.env.DB_DIR || 'db'
const db = open({path: dbDir})

const writeLocks = new Map()
const writeLocksLock = makeLock()

const bigIntPrefix = 'B:'
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
  const key = `/${networkName}${path}`
  if (result === undefined) return deserialise(db.get(key))
  else await db.put(key, serialise(result))
}

async function cachedData(name, epoch, data) {
  const key = `/${networkName}/${name}/${epoch}`
  if (typeof data != 'undefined') {
    if (data === 'check')
      return db.doesExist(key)
    else
      await db.put(key, data)
  }
  else
    return db.get(key)
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
  const path = `/eth/v1/beacon/blocks/${slotNumber}/attestations`
  const key = `${path}/attestations`
  const cache = await cachedBeacon(key); if (cache !== undefined) return cache
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200)
    console.warn(`Unexpected response status getting ${slotNumber} attestations: ${response.status}: ${await response.text()}`)
  const json = await response.json()
  const result = json.data.map(
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
  if (response.status === 404)
    response.json = async () => {return {data: {validator: {activation_epoch: 'FAR_FUTURE_EPOCH', exit_epoch: 'FAR_FUTURE_EPOCH'}}}}
  else if (response.status !== 200)
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
  if (response.status === 404)
    return -1
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
  const writeLock = await writeLocksLock(() => {
    if (!writeLocks.has(key)) writeLocks.set(key, {refs: 0, lock: makeLock()})
    const writeLock = writeLocks.get(key)
    writeLock.refs++
    return writeLock
  })
  return await writeLock.lock(async () => {
    let result = db.get(key)
    if (result !== undefined)
      result = deserialise(result)
    else {
      result = await contract[fn](...args, {blockTag})
      await db.put(key, serialise(result))
    }
    writeLock.refs--
    if (!writeLock.refs)
      await writeLocksLock(() => writeLocks.delete(key))
    return result
  })
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
     'function getNodeFee() view returns (uint256)',
     'function getNodeAddress() view returns (address)'
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

const rocketRewardsPoolABI = [
  'function getClaimIntervalTimeStart() view returns (uint256)',
  'function getClaimIntervalTime() view returns (uint256)',
  'function getPendingRPLRewards() view returns (uint256)',
  'function getClaimingContractPerc(string) view returns (uint256)',
  'function getRewardIndex() view returns (uint256)',
  // struct RewardSubmission {uint256 rewardIndex; uint256 executionBlock; uint256 consensusBlock; bytes32 merkleRoot; string merkleTreeCID; uint256 intervalsPassed; uint256 treasuryRPL; uint256[] trustedNodeRPL; uint256[] nodeRPL; uint256[] nodeETH; uint256 userETH;}
  'event RewardSnapshot(uint256 indexed rewardIndex, '+
   '(uint256, uint256, uint256, bytes32, string,' +
   ' uint256, uint256, uint256[], uint256[], uint256[], uint256) submission, ' +
   'uint256 intervalStartTime, uint256 intervalEndTime, uint256 time)'
]

const rocketNodeManagerABI = [
  'function getNodeCount() view returns (uint256)',
  'function getNodeAt(uint256) view returns (address)',
  'function getNodeRegistrationTime(address) view returns (uint256)',
  'function getSmoothingPoolRegistrationState(address) view returns (bool)',
  'function getSmoothingPoolRegistrationChanged(address) view returns (uint256)'
]

const rocketMinipoolManagerABI = [
  'function getNodeMinipoolAt(address, uint256) view returns (address)',
  'function getNodeMinipoolCount(address) view returns (uint256)',
  'function getMinipoolPubkey(address) view returns (bytes)',
  'function getMinipoolCount() view returns (uint256)'
]

const minipoolABI = [
  'function getStatus() view returns (uint8)',
  'function getStatusTime() view returns (uint256)',
  'function getUserDepositBalance() view returns (uint256)',
  'function getNodeDepositBalance() view returns (uint256)',
  'function getNodeFee() view returns (uint256)',
  'function getNodeAddress() view returns (address)'
]

const multicaller = new MulticallProvider(provider)
const MAX_CALLS = 128

const getELState = async (blockTag) => {
  const rocketStorage = new ethers.Contract(
    rocketStorageAddresses.get(networkName),
    ['function getAddress(bytes32) view returns (address)'], multicaller)
  const getRocketAddress = (name) =>
    rocketStorage['getAddress(bytes32)'](ethers.id(`contract.address${name}`), {blockTag})
  const contractInfo = [
    ['rocketRewardsPool', rocketRewardsPoolABI],
    ['rocketNodeManager', rocketNodeManagerABI],
    ['rocketNodeStaking', ['function getNodeRPLStake(address) view returns (uint256)']],
    ['rocketMinipoolManager', rocketMinipoolManagerABI],
    ['rocketNetworkPrices', ['function getRPLPrice() view returns (uint256)']],
    ['rocketDAOProtocolSettingsNode', ['function getMinimumPerMinipoolStake() view returns (uint256)',
                                       'function getMaximumPerMinipoolStake() view returns (uint256)']],
    ['rocketDAONodeTrusted', ['function getMemberCount() view returns (uint256)',
                              'function getMemberAt(uint256) view returns (address)',
                              'function getMemberJoinedTime(address) view returns (uint256)']],
    ['rocketNetworkPenalties', ['function getPenaltyCount(address) view returns (uint256)']],
    ['rocketMinipoolBondReducer', ['function getLastBondReductionPrevValue(address) view returns (uint256)',
                                   'function getLastBondReductionPrevNodeFee(address) view returns (uint256)',
                                   'function getLastBondReductionTime(address) view returns (uint256)']]
  ]
  const contracts = new Map()
  const promises = []
  const drainPromises = () => Promise.all(promises.splice(0, promises.length))
  const enqueuePromises = async (...ps) => {
    if (promises.length + ps.length > MAX_CALLS)
      await drainPromises()
    promises.push(...ps)
  }
  await enqueuePromises(...contractInfo.map(([name, abi]) =>
    getRocketAddress(name).then(address =>
      contracts.set(name, new ethers.Contract(address, abi, multicaller))
    )
  ))
  log(3, 'Getting contract addresses')
  await drainPromises()
  const rocketDAONodeTrusted = contracts.get('rocketDAONodeTrusted')
  const rocketNodeManager = contracts.get('rocketNodeManager')
  const rocketMinipoolManager = contracts.get('rocketMinipoolManager')
  const rocketNetworkPenalties = contracts.get('rocketNetworkPenalties')
  const rocketMinipoolBondReducer = contracts.get('rocketMinipoolBondReducer')
  const rocketNodeStaking = contracts.get('rocketNodeStaking')
  const state = {
    'rocketRewardsPool': {
      'getClaimIntervalTimeStart': {'': null},
      'getClaimIntervalTime': {'': null},
      'getRewardIndex': {'': null},
      'getPendingRPLRewards': {'': null},
      'getClaimingContractPerc': {
        'rocketClaimNode': null,
        'rocketClaimTrustedNode': null,
        'rocketClaimDAO': null
      },
    },
    'rocketDAONodeTrusted': {
      'getMemberCount': {'': null},
      'getMemberAt': {},
      'getMemberJoinedTime': {}
    },
    'rocketNodeManager': {
      'getNodeCount': {'': null},
      'getNodeAt': {},
      'getNodeRegistrationTime': {},
      'getSmoothingPoolRegistrationState': {},
      'getSmoothingPoolRegistrationChanged': {}
    },
    'rocketMinipoolManager': {
      'getMinipoolCount': {'': null},
      'getNodeMinipoolCount': {},
      'getNodeMinipoolAt': {},
      'getMinipoolPubkey': {}
    },
    'rocketNetworkPrices': {
      'getRPLPrice': {'': null}
    },
    'rocketDAOProtocolSettingsNode': {
      'getMinimumPerMinipoolStake': {'': null}
    },
    'rocketMinipoolBondReducer': {
      'getLastBondReductionPrevValue': {},
      'getLastBondReductionPrevNodeFee': {},
      'getLastBondReductionTime': {}
    },
    'rocketNetworkPenalties': {
      'getPenaltyCount': {}
    },
    'rocketNodeStaking': {
      'getNodeRPLStake': {},
    }
  }
  for (const [name, data] of Object.entries(state)) {
    const c = contracts.get(name)
    for (const [fn, calls] of Object.entries(data))
      for (const args of Object.keys(calls))
        await enqueuePromises(
          c[fn](...(args ? [args, {blockTag}] : [{blockTag}])).then(r =>
            calls[args] = r
          )
        )
  }
  log(3, 'Getting indepedent calls')
  await drainPromises()
  const nodeCount = state['rocketNodeManager']['getNodeCount']['']
  for (let i = 0; i < nodeCount; i++)
    await enqueuePromises(
      rocketNodeManager['getNodeAt'](i, {blockTag}).then(r =>
        state['rocketNodeManager']['getNodeAt'][i] = r
      )
    )
  const memberCount = state['rocketDAONodeTrusted']['getMemberCount']['']
  for (let i = 0; i < memberCount; i++)
    await enqueuePromises(
      rocketDAONodeTrusted['getMemberAt'](i, {blockTag}).then(r =>
        state['rocketDAONodeTrusted']['getMemberAt'][i] = r
      )
    )
  log(3, 'Getting node addresses')
  await drainPromises()
  const nodeAddresses = []
  for (let i = 0; i < nodeCount; i++)
    nodeAddresses.push(state['rocketNodeManager']['getNodeAt'][i])
  const members = []
  for (let i = 0; i < memberCount; i++)
    members.push(state['rocketDAONodeTrusted']['getMemberAt'][i])
  for (const member of members)
    await enqueuePromises(
      rocketDAONodeTrusted['getMemberJoinedTime'](member, {blockTag}).then(r =>
        state['rocketDAONodeTrusted']['getMemberJoinedTime'][member] = r
      )
    )
  for (const nodeAddress of nodeAddresses)
    await enqueuePromises(
      rocketMinipoolManager.getNodeMinipoolCount(nodeAddress, {blockTag}).then(r =>
        state['rocketMinipoolManager']['getNodeMinipoolCount'][nodeAddress] = r
      ),
      rocketNodeStaking.getNodeRPLStake(nodeAddress, {blockTag}).then(r =>
        state['rocketNodeStaking']['getNodeRPLStake'][nodeAddress] = r
      ),
      rocketNodeManager.getNodeRegistrationTime(nodeAddress, {blockTag}).then(r =>
        state['rocketNodeManager']['getNodeRegistrationTime'][nodeAddress] = r
      ),
      rocketNodeManager.getSmoothingPoolRegistrationState(nodeAddress, {blockTag}).then(r =>
        state['rocketNodeManager']['getSmoothingPoolRegistrationState'][nodeAddress] = r
      ),
      rocketNodeManager.getSmoothingPoolRegistrationChanged(nodeAddress, {blockTag}).then(r =>
        state['rocketNodeManager']['getSmoothingPoolRegistrationChanged'][nodeAddress] = r
      )
    )
  log(3, 'Getting node data')
  await drainPromises()
  for (const nodeAddress of nodeAddresses) {
    const nodeMinipoolCount = state['rocketMinipoolManager']['getNodeMinipoolCount'][nodeAddress]
    for (let i = 0; i < nodeMinipoolCount; i++)
      await enqueuePromises(
        rocketMinipoolManager.getNodeMinipoolAt(nodeAddress, i, {blockTag}).then(r =>
          state['rocketMinipoolManager']['getNodeMinipoolAt'][`${nodeAddress},${i}`] = r
        )
      )
  }
  log(3, 'Getting minipool addresses')
  await drainPromises()
  const minipools = []
  for (const nodeAddress of nodeAddresses) {
    const nodeMinipoolCount = state['rocketMinipoolManager']['getNodeMinipoolCount'][nodeAddress]
    for (let i = 0; i < nodeMinipoolCount; i++)
      minipools.push(state['rocketMinipoolManager']['getNodeMinipoolAt'][`${nodeAddress},${i}`])
  }
  for (const minipool of minipools) {
    const c = new ethers.Contract(minipool, minipoolABI, multicaller)
    state[minipool] = {}
    await enqueuePromises(
      c.getNodeAddress({blockTag}).then(r => state[minipool]['getNodeAddress'] = r),
      c.getStatus({blockTag}).then(r => state[minipool]['getStatus'] = r),
      c.getStatusTime({blockTag}).then(r => state[minipool]['getStatusTime'] = r),
      c.getUserDepositBalance({blockTag}).then(r => state[minipool]['getUserDepositBalance'] = r),
      c.getNodeDepositBalance({blockTag}).then(r => state[minipool]['getNodeDepositBalance'] = r),
      c.getNodeFee({blockTag}).then(r => state[minipool]['getNodeFee'] = r),
      rocketMinipoolManager.getMinipoolPubkey(minipool, {blockTag}).then(r =>
        state['rocketMinipoolManager']['getMinipoolPubkey'][minipool] = r
      ),
      rocketNetworkPenalties.getPenaltyCount(minipool, {blockTag}).then(r =>
        state['rocketNetworkPenalties']['getPenaltyCount'][minipool] = r
      ),
      rocketMinipoolBondReducer.getLastBondReductionPrevValue(minipool, {blockTag}).then(r =>
        state['rocketMinipoolBondReducer']['getLastBondReductionPrevValue'][minipool] = r
      ),
      rocketMinipoolBondReducer.getLastBondReductionPrevNodeFee(minipool, {blockTag}).then(r =>
        state['rocketMinipoolBondReducer']['getLastBondReductionPrevNodeFee'][minipool] = r
      ),
      rocketMinipoolBondReducer.getLastBondReductionTime(minipool, {blockTag}).then(r =>
        state['rocketMinipoolBondReducer']['getLastBondReductionTime'][minipool] = r
      )
    )
  }
  log(3, 'Getting minipool data')
  await drainPromises()
  return state
}

const multicallAddresses = new Map()
multicallAddresses.set('mainnet', '0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441')
multicallAddresses.set('goerli', '0x77dCa2C955b15e9dE4dbBCf1246B4B85b651e50e')

const multicallContract = new ethers.Contract(
  multicallAddresses.get(networkName),
  ['function aggregate((address, bytes)[]) view returns (uint256, bytes[])'],
  provider)
log(3, `Using multicall ${await multicallContract.getAddress()}`)
contracts.set('multicall', multicallContract)

async function multicall(calls, blockTag) {
  const aggregateArgs = []
  for (const {contract, fn, args} of calls) {
    aggregateArgs.push([
      await contract.getAddress(),
      contract.interface.encodeFunctionData(fn, args)
    ])
  }
  const aggregateBytes = await multicallContract.aggregate(
    aggregateArgs, {blockTag}
  ).then(r => Array.from(r[1]))
  const results = []
  for (const [i, bytes] of aggregateBytes.entries()) {
    const {contract, fn} = calls[i]
    results.push(
      contract.interface.decodeFunctionResult(fn, bytes)[0]
    )
  }
  return results
}

const rocketStorage = new ethers.Contract(
  rocketStorageAddresses.get(networkName),
  ['function getAddress(bytes32) view returns (address)'], provider)
log(2, `Using Rocket Storage ${await rocketStorage.getAddress()}`)
contracts.set('rocketStorage', rocketStorage)

const getRocketAddress = (name, blockTag) =>
  cachedCall(rocketStorage, 'getAddress(bytes32)', [ethers.id(`contract.address${name}`)], blockTag)

const rocketRewardsPool = new ethers.Contract(
  await getRocketAddress('rocketRewardsPool', startBlock),
  rocketRewardsPoolABI,
  provider)
contracts.set('rocketRewardsPool', rocketRewardsPool)

log(2, `startBlock: ${startBlock}`)
const startTime = await rocketRewardsPool.getClaimIntervalTimeStart()
log(1, `startTime: ${startTime}`)

const intervalTime = await cachedCall(rocketRewardsPool, 'getClaimIntervalTime', [], startBlock)
log(2, `intervalTime: ${intervalTime}`)

let targetBcSlot
const targetSlotEpochOverride = tryBigInt(process.env.OVERRIDE_TARGET_EPOCH)

if (!targetSlotEpochOverride) {

  const latestBlockTime = await provider.getBlock('latest').then(b => BigInt(b.timestamp))
  log(2, `latestBlockTime: ${latestBlockTime}`)

  const timeSinceStart = latestBlockTime - startTime
  const intervalsPassed = timeSinceStart / intervalTime
  log(2, `intervalsPassed: ${intervalsPassed}`)

  const endTime = startTime + (intervalTime * intervalsPassed)
  log(1, `endTime: ${endTime}`)

  const totalTimespan = endTime - genesisTime
  log(2, `totalTimespan: ${totalTimespan}`)

  targetBcSlot = totalTimespan / secondsPerSlot
  if (totalTimespan % secondsPerSlot) targetBcSlot++

}

const targetSlotEpoch = targetSlotEpochOverride || targetBcSlot / slotsPerEpoch
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

log(3, `fetching EL state...`)
const elState = await getELState(targetElBlock)
log(3, `fetched EL state`)

const targetElBlockTimestamp = await provider.getBlock(targetElBlock).then(b => BigInt(b.timestamp))
log(2, `targetElBlockTimestamp: ${targetElBlockTimestamp}`)

const rocketNodeManager = new ethers.Contract(
  await getRocketAddress('rocketNodeManager', targetElBlock),
  rocketNodeManagerABI,
  provider)
contracts.set('rocketNodeManager', rocketNodeManager)

const rocketNodeStaking = new ethers.Contract(
  await getRocketAddress('rocketNodeStaking', targetElBlock),
  ['function getNodeRPLStake(address) view returns (uint256)'],
  provider)
contracts.set('rocketNodeStaking', rocketNodeStaking)

const rocketMinipoolManager = new ethers.Contract(
  await getRocketAddress('rocketMinipoolManager', targetElBlock),
  rocketMinipoolManagerABI,
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

const rocketSmoothingPool = await getRocketAddress('rocketSmoothingPool', targetElBlock)
contracts.set('rocketSmoothingPool', rocketSmoothingPool)

const smoothingPoolBalance = await provider.getBalance(rocketSmoothingPool, targetElBlock)
log(2, `smoothingPoolBalance: ${smoothingPoolBalance}`)

async function nodeSmoothingTimes(nodeAddress, blockTag, times) {
  const key = `/${networkName}/${blockTag}/nodeSmoothingTimes/${nodeAddress}`
  if (times) {
    if (times === 'check')
      return db.doesExist(key)
    else
      await db.put(key, times)
  }
  else {
    return db.get(key)
  }
}

const currentIndex = elState['rocketRewardsPool']['getRewardIndex']['']
const previousIntervalEventFilter = rocketRewardsPool.filters.RewardSnapshot(currentIndex - 1n)
const intervalBlocksApprox = intervalTime / 12n
const foundEvents = await rocketRewardsPool.queryFilter(previousIntervalEventFilter, targetElBlock - intervalBlocksApprox, targetElBlock)
if (foundEvents.length !== 1)
  throw new Error(`Did not find exactly 1 RewardSnapshot event for Interval ${currentIndex - 1n}`)
const previousIntervalEvent = foundEvents.pop()
const RewardSubmission = previousIntervalEvent.args[1]
const ExecutionBlock = RewardSubmission[1]
const ConsensusBlock = RewardSubmission[2]

const dataKeys = ['duties', 'attestations', 'scores']

const cachePort = workerData
cachePort.on('message', async ({id, request: splits}) => {
  if (splits.length >= 3 && splits.length <= 4 && splits[0] == 'elState') {
    const [contractName, fn, args] = splits.slice(1)
    const data = elState[contractName][fn]
    const value = contractName.startsWith('0x') ? data : data[args || '']
    const response = typeof value === 'bigint' ? value.toString() : value
    cachePort.postMessage({id, response})
  }
  else if (splits.length == 3 && splits[0] == 'multicall') {
    const [namedCalls, blockTagName] = splits.slice(1)
    const blockTag = blockTagName == 'targetElBlock' ? targetElBlock : blockTagName
    const calls = namedCalls.map(call => ({contract: getContract(call.contractName), ...call}))
    const response = await multicall(calls, blockTag)
    cachePort.postMessage({id, response})
  }
  else if (splits.length == 3 && splits[0] == 'beacon') {
    if (splits[1] == 'getAttestationsFromSlot')
      cachePort.postMessage({id, response: await getAttestationsFromSlot(splits[2])})
    else if (splits[1] == 'getValidatorStatus')
      cachePort.postMessage({id, response: await getValidatorStatus(targetBcSlot, splits[2])})
    else if (splits[1] == 'getIndexFromPubkey')
      cachePort.postMessage({id, response: await getIndexFromPubkey(splits[2])})
    else if (splits[1] == 'getCommittees')
      cachePort.postMessage({id, response: await getCommittees(splits[2])})
    else if (splits[1] == 'checkSlotExists')
      cachePort.postMessage({id, response: await checkSlotExists(splits[2])})
    else if (splits[1] == 'getBlockNumberFromSlot')
      cachePort.postMessage({id, response: await getBlockNumberFromSlot(splits[2])})
    else
      cachePort.postMessage({id, error: `invalid request: unknown beacon request ${splits[1]}`})
  }
  else if (splits.length == 3 && splits[0] == 'nodeSmoothingTimes' && splits[2] == 'check')
    cachePort.postMessage({id, response: await nodeSmoothingTimes(splits[1], targetElBlock, splits[2])})
  else if (splits.length == 4 && splits[0] == 'nodeSmoothingTimes') {
    await nodeSmoothingTimes(splits[1], targetElBlock, {optInTime: splits[2], optOutTime: splits[3]})
    cachePort.postMessage({id, response: 'success'})
  }
  else if (splits.length == 2 && splits[0] == 'nodeSmoothingTimes')
    cachePort.postMessage({id, response: await nodeSmoothingTimes(splits[1], targetElBlock)})
  else if (splits.length == 3 && dataKeys.includes(splits[0]) && splits[2] == 'check')
    cachePort.postMessage({id, response: await cachedData(splits[0], splits[1], splits[2])})
  else if (splits.length == 3 && dataKeys.includes(splits[0]))
    cachePort.postMessage({id, response: await cachedData(splits[0], splits[1], splits[2])})
  else if (splits.length == 2 && dataKeys.includes(splits[0]))
    cachePort.postMessage({id, response: await cachedData(splits[0], splits[1])})
  else if (splits.length == 1 && splits[0] == 'ExecutionBlock')
    cachePort.postMessage({id, response: ExecutionBlock})
  else if (splits.length == 1 && splits[0] == 'ConsensusBlock')
    cachePort.postMessage({id, response: ConsensusBlock})
  else if (splits.length == 1 && splits[0] == 'targetElBlockTimestamp')
    cachePort.postMessage({id, response: targetElBlockTimestamp})
  else if (splits.length == 1 && splits[0] == 'targetSlotEpoch')
    cachePort.postMessage({id, response: targetSlotEpoch})
  else if (splits.length == 1 && splits[0] == 'targetBcSlot')
    cachePort.postMessage({id, response: targetBcSlot})
  else if (splits.length == 1 && splits[0] == 'targetElBlock')
    cachePort.postMessage({id, response: targetElBlock})
  else if (splits.length == 1 && splits[0] == 'currentIndex')
    cachePort.postMessage({id, response: currentIndex})
  else if (splits.length == 1 && splits[0] == 'intervalTime')
    cachePort.postMessage({id, response: intervalTime})
  else if (splits.length == 1 && splits[0] == 'smoothingPoolBalance')
    cachePort.postMessage({id, response: smoothingPoolBalance})
  else if (splits.length == 1 && splits[0] == 'close')
    await db.close()
  else
    cachePort.postMessage({id, error: 'invalid request'})
})
