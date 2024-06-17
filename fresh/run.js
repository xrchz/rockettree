import 'dotenv/config'
import { ethers } from 'ethers'
import { open } from 'lmdb'
import { readFileSync, writeFileSync } from 'node:fs'
import { MulticallProvider } from "@ethers-ext/provider-multicall"

const dbDir = process.env.DB_DIR || 'db'
const db = open({path: dbDir})

function tryBigInt(s) { try { return BigInt(s) } catch { return false } }

const timestamp = () => Intl.DateTimeFormat('en-GB',
  {hour: 'numeric', minute: 'numeric', second: 'numeric'})
  .format(new Date())

const verbosity = parseInt(process.env.VERBOSITY) || 2
const log = (v, s) => verbosity >= v && console.log(`${timestamp()}: ${s}`)

const genesisTime = 1606824023n
const rocketStorageAddress = '0x1d8f8f00cfa6758d7bE78336684788Fb0ee0Fa46'
const secondsPerSlot = 12n
const slotsPerEpoch = 32n
const rpip30Interval = 18
const denebEpoch = 269568
const stakingStatus = 2

const thirteen6137Ether = ethers.parseEther('13.6137')
const oneEther = ethers.parseEther('1')
const twoEther = 2n * oneEther
const oneHundredEther = 100n * oneEther
const thirteenEther = 13n * oneEther
const fifteenEther = 15n * oneEther
function log2(x) {
  const exponent = BigInt(Math.floor(Math.log2(parseInt(x / oneEther))))
  let result = exponent * oneEther
  let y = x >> exponent
  if (y == oneEther) return result
  let delta = oneEther
  for (const i of Array(60).keys()) {
    delta = delta / 2n
    y = (y * y) / oneEther
    if (y >= twoEther) {
      result = result + delta
      y = y / 2n
    }
  }
  return result
}
const ln = (x) => (log2(x) * oneEther) / 1442695040888963407n

const beaconRpcUrl = process.env.BN_URL || 'http://localhost:5052'
const provider = new ethers.JsonRpcProvider(process.env.RPC_URL || 'http://localhost:8545')
const networkName = await provider.getNetwork().then(n => n.name)
const multicaller = new MulticallProvider(provider)
const MAX_CALLS = parseInt(process.env.MAX_CALLS) || 128

const rocketRewardsPoolABI = [
  'function getClaimIntervalTimeStart() view returns (uint256)',
  'function getClaimIntervalTime() view returns (uint256)',
  'function getPendingRPLRewards() view returns (uint256)',
  'function getClaimingContractPerc(string) view returns (uint256)',
  'function getRewardIndex() view returns (uint256)',
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

const getELState = async (blockTag) => {
  const cacheFilename = `cache/el-${blockTag}.json`
  try { return JSON.parse(readFileSync(cacheFilename)) } catch { }
  log(3, `Using ${blockTag} throughout getELState`)
  const rocketStorage = new ethers.Contract(rocketStorageAddress,
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
      for (const arg of Object.keys(calls)) {
        await enqueuePromises(
          c[fn](...(arg ? [arg, {blockTag}] : [{blockTag}])).then(r =>
            calls[arg] = r
          )
        )
      }
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
  writeFileSync(cacheFilename,
    JSON.stringify(state,
      (_, v) => typeof v == 'bigint' ? `0x${v.toString(16)}` : v
    )
  )
  return state
}

const startBlock = parseInt(process.env.START_BLOCK) || 'latest'
const latestBlock = parseInt(process.env.LATEST_BLOCK) || 'latest'
log(2, `startBlock: ${startBlock}`)
log(2, `latestBlock: ${latestBlock}`)

const rocketStorage = new ethers.Contract(rocketStorageAddress,
  ['function getAddress(bytes32) view returns (address)'], provider)
const getRocketAddress = (name, blockTag) =>
  rocketStorage['getAddress(bytes32)'](ethers.id(`contract.address${name}`), {blockTag})
const rocketRewardsPool = new ethers.Contract(
  await getRocketAddress('rocketRewardsPool', startBlock), rocketRewardsPoolABI, provider
)

const startTime = await rocketRewardsPool.getClaimIntervalTimeStart({blockTag: startBlock})
log(1, `startTime: ${startTime}`)

const intervalTime = await rocketRewardsPool.getClaimIntervalTime({blockTag: startBlock})
log(2, `intervalTime: ${intervalTime}`)

let targetBcSlot
const targetSlotEpochOverride = tryBigInt(process.env.OVERRIDE_TARGET_EPOCH)

if (!targetSlotEpochOverride) {

  const latestBlockTime = await provider.getBlock(latestBlock).then(b => BigInt(b.timestamp))
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

const targetElBlock = await getBlockNumberFromSlot(targetBcSlot)
log(1, `targetElBlock: ${targetElBlock}`)

const targetElBlockTimestamp = await provider.getBlock(targetElBlock).then(b => BigInt(b.timestamp))
log(2, `targetElBlockTimestamp: ${targetElBlockTimestamp}`)

log(3, `fetching EL state...`)
const elState = await getELState(targetElBlock)
log(3, `fetched EL state`)

const currentIndex = BigInt(elState['rocketRewardsPool']['getRewardIndex'][''])
log(2, `currentIndex: ${currentIndex}`)

const pendingRewards = BigInt(elState['rocketRewardsPool']['getPendingRPLRewards'][''])
const collateralPercent = BigInt(elState['rocketRewardsPool']['getClaimingContractPerc']['rocketClaimNode'])
const oDaoPercent = BigInt(elState['rocketRewardsPool']['getClaimingContractPerc']['rocketClaimTrustedNode'])
const pDaoPercent = BigInt(elState['rocketRewardsPool']['getClaimingContractPerc']['rocketClaimDAO'])

const _100Percent = ethers.parseEther('1')
const collateralRewards = pendingRewards * collateralPercent / _100Percent
const oDaoRewards = pendingRewards * oDaoPercent / _100Percent
const pDaoRewards = pendingRewards * pDaoPercent / _100Percent
log(2, `pendingRewards: ${pendingRewards}`)
log(2, `collateralRewards: ${collateralRewards}`)
log(2, `oDaoRewards: ${oDaoRewards}`)
log(2, `pDaoRewards: ${pDaoRewards}`)

const nodeCount = BigInt(elState['rocketNodeManager']['getNodeCount'][''])
log(2, `nodeCount: ${nodeCount}`)
const nodeIndices = Array.from(Array(parseInt(nodeCount)).keys())
const nodeAddresses = nodeIndices.map(i => elState['rocketNodeManager']['getNodeAt'][i])
log(3, `nodeAddresses: ${nodeAddresses.slice(0, 5)}...`)

const nodeEffectiveStakes = new Map()
const nodeWeights = new Map()
let totalEffectiveRplStake = 0n
let totalNodeWeight = 0n
const rpip30C = BigInt(Math.min(6, parseInt(currentIndex) - rpip30Interval + 1))

const allPubkeys = Array.from(new Set(Object.values(elState['rocketMinipoolManager']['getMinipoolPubkey'])))
log(3, `allPubkeys: ${allPubkeys.slice(0, 5)}...`)
log(3, `fetching validator statuses...`)
const validatorStatuses = {}
{
  const cacheFilename = `cache/vs-${targetBcSlot}.json`
  try {
    Object.assign(validatorStatuses, JSON.parse(readFileSync(cacheFilename)))
  }
  catch {
    const path = `/eth/v1/beacon/states/${targetBcSlot}/validators`
    const url = new URL(`${beaconRpcUrl}${path}`)
    const options =  {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ids: allPubkeys})
    }
    const response = await fetch(url, options)
    if (response.status !== 200) {
      console.error(`Unexpected response getting validator statuses: ${response.status}: ${await response.text()}`)
      process.exit(1)
    }
    const items = await response.json().then(j => j.data)
    for (const {validator: {pubkey, activation_epoch, exit_epoch}} of items)
      validatorStatuses[pubkey] = {activation_epoch, exit_epoch}
    writeFileSync(cacheFilename, JSON.stringify(validatorStatuses))
  }
}
log(3, `fetched validator statuses`)

function getEligibility(activationEpoch, exitEpoch) {
  const deposited = activationEpoch != 'FAR_FUTURE_EPOCH'
  const activated = deposited && BigInt(activationEpoch) < targetSlotEpoch
  const notExited = exitEpoch == 'FAR_FUTURE_EPOCH' || targetSlotEpoch < BigInt(exitEpoch)
  return deposited && (currentIndex >= 15n || activated) && notExited
}

function getNodeWeight(eligibleBorrowedEth, nodeStake) {
  if (currentIndex < rpip30Interval) return 0n
  if (!eligibleBorrowedEth) return 0n
  const stakedRplValueInEth = nodeStake * ratio / oneEther
  const percentOfBorrowedEth = stakedRplValueInEth * oneHundredEther / eligibleBorrowedEth
  if (percentOfBorrowedEth <= fifteenEther)
    return 100n * stakedRplValueInEth
  else
    return ((thirteen6137Ether + 2n * ln(percentOfBorrowedEth - thirteenEther)) * eligibleBorrowedEth) / oneEther
}

const ratio = BigInt(elState['rocketNetworkPrices']['getRPLPrice'][''])
const minCollateralFraction = BigInt(elState['rocketDAOProtocolSettingsNode']['getMinimumPerMinipoolStake'][''])
const maxCollateralFraction = 150n * 10n ** 16n

for (const nodeAddress of nodeAddresses) {
  const minipoolCount = BigInt(elState['rocketMinipoolManager']['getNodeMinipoolCount'][nodeAddress])
  const minipoolIndicesToProcess = Array.from(Array(parseInt(minipoolCount)).keys())
  const nodeStake = BigInt(elState['rocketNodeStaking']['getNodeRPLStake'][nodeAddress])
  const registrationTime = BigInt(elState['rocketNodeManager']['getNodeRegistrationTime'][nodeAddress])
  let eligibleBorrowedEth = 0n
  let eligibleBondedEth = 0n
  for (const i of Array(parseInt(minipoolCount)).keys()) {
    const minipoolAddress = elState['rocketMinipoolManager']['getNodeMinipoolAt'][`${nodeAddress},${i}`]
    const minipoolStatus = BigInt(elState[minipoolAddress]['getStatus'])
    if (minipoolStatus != stakingStatus) continue
    const pubkey = elState['rocketMinipoolManager']['getMinipoolPubkey'][minipoolAddress]
    const {activation_epoch, exit_epoch} = validatorStatuses[pubkey]
    if (getEligibility(activation_epoch, exit_epoch)) {
      eligibleBorrowedEth += BigInt(elState[minipoolAddress]['getUserDepositBalance'])
      eligibleBondedEth += BigInt(elState[minipoolAddress]['getNodeDepositBalance'])
    }
  }
  const minCollateral = eligibleBorrowedEth * minCollateralFraction / ratio
  const maxCollateral = eligibleBondedEth * maxCollateralFraction / ratio
  let [nodeEffectiveStake, nodeWeight] = [0n, 0n]
  if (minCollateral <= nodeStake) {
    nodeEffectiveStake = nodeStake < maxCollateral ? nodeStake : maxCollateral
    nodeWeight = getNodeWeight(eligibleBorrowedEth, nodeStake)
  }
  const nodeAge = targetElBlockTimestamp - registrationTime
  if (nodeAge < intervalTime) {
    nodeEffectiveStake = nodeEffectiveStake * nodeAge / intervalTime
    nodeWeight = nodeWeight * nodeAge / intervalTime
  }
  nodeEffectiveStakes.set(nodeAddress, nodeEffectiveStake)
  totalEffectiveRplStake += nodeEffectiveStake
  if (rpip30Interval <= currentIndex) {
    nodeWeights.set(nodeAddress, nodeWeight)
    totalNodeWeight += nodeWeight
  }
}

log(1, `totalEffectiveRplStake: ${totalEffectiveRplStake}`)
log(1, `totalNodeWeight: ${totalNodeWeight}`)

const numberOfMinipools = BigInt(elState['rocketMinipoolManager']['getMinipoolCount'][''])

const nodeCollateralAmounts = new Map()
let totalCalculatedCollateralRewards = 0n

if (totalEffectiveRplStake && (currentIndex < rpip30Interval || totalNodeWeight)) {
  for (const nodeAddress of nodeAddresses) {
    const nodeEffectiveStake = nodeEffectiveStakes.get(nodeAddress)
    const nodeWeight = nodeWeights.get(nodeAddress)
    const nodeCollateralAmount = currentIndex < rpip30Interval ?
      collateralRewards * nodeEffectiveStake / totalEffectiveRplStake :
      (collateralRewards * rpip30C * nodeWeight / (totalNodeWeight * 6n)) +
      (collateralRewards * (6n - rpip30C) * nodeEffectiveStake / (totalEffectiveRplStake * 6n))
    nodeCollateralAmounts.set(nodeAddress, nodeCollateralAmount)
    totalCalculatedCollateralRewards += nodeCollateralAmount
  }
}
else {
  totalCalculatedCollateralRewards = collateralRewards
}

log(1, `totalCalculatedCollateralRewards: ${totalCalculatedCollateralRewards}`)
if (collateralRewards - totalCalculatedCollateralRewards > numberOfMinipools)
  throw new Error('collateral calculation has excessive error')

const oDaoCount = BigInt(elState['rocketDAONodeTrusted']['getMemberCount'][''])
const oDaoIndices = Array.from(Array(parseInt(oDaoCount)).keys())
const oDaoAddresses = oDaoIndices.map(i => elState['rocketDAONodeTrusted']['getMemberAt'][i])
log(3, `oDaoAddresses: ${oDaoAddresses.slice(0, 5)}...`)

let totalParticipatedSeconds = 0n
const oDaoParticipatedSeconds = new Map()

for (const nodeAddress of oDaoAddresses) {
  const joinTime = BigInt(elState['rocketDAONodeTrusted']['getMemberJoinedTime'][nodeAddress])
  const odaoTime = targetElBlockTimestamp - joinTime
  const participatedSeconds = odaoTime < intervalTime ? odaoTime : intervalTime
  oDaoParticipatedSeconds.set(nodeAddress, participatedSeconds)
  totalParticipatedSeconds += participatedSeconds
}

log(2, `totalParticipatedSeconds: ${totalParticipatedSeconds}`)

let totalCalculatedODaoRewards = 0n
const oDaoAmounts = new Map()
for (const nodeAddress of oDaoAddresses) {
  const participatedSeconds = oDaoParticipatedSeconds.get(nodeAddress)
  const oDaoAmount = oDaoRewards * participatedSeconds / totalParticipatedSeconds
  oDaoAmounts.set(nodeAddress, oDaoAmount)
  totalCalculatedODaoRewards += oDaoAmount
}
log(1, `totalCalculatedODaoRewards: ${totalCalculatedODaoRewards}`)

if (oDaoRewards - totalCalculatedODaoRewards > numberOfMinipools)
  throw new Error('oDAO calculation has excessive error')

const actualPDaoRewards = pendingRewards - totalCalculatedCollateralRewards - totalCalculatedODaoRewards
log(1, `actualPDaoRewards: ${actualPDaoRewards}`)
log(3, `pDAO rewards delta: ${actualPDaoRewards - pDaoRewards}`)
