import 'dotenv/config'
import { ethers } from 'ethers'
import { readFileSync, writeFileSync, existsSync } from 'node:fs'
import { MulticallProvider } from "@ethers-ext/provider-multicall"

function tryBigInt(s) { try { return BigInt(s) } catch { return false } }

const max = (a, b) => a < b ? b : a
const min = (a, b) => a <= b ? a : b

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
const farPastTime = 0n
const farFutureTime = BigInt(1e18)

const thirteen6137Ether = ethers.parseEther('13.6137')
const oneEther = ethers.parseEther('1')
const oneGwei = ethers.parseUnits('1', 'gwei')
const tenEther = 10n * oneEther
const oneTenthEther = oneEther / 10n
const oneFourHundredthEther = 4n * oneEther / 100n
const twoEther = 2n * oneEther
const oneHundredEther = 100n * oneEther
const thirteenEther = 13n * oneEther
const fifteenEther = 15n * oneEther
const thirtyTwoEther = 32n * oneEther
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

const stringifier = (_, v) => typeof v == 'bigint' ? `0x${v.toString(16)}` : v

const perfUrl = process.env.PERF_URL || 'http://localhost:8789'
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
                                   'function getLastBondReductionTime(address) view returns (uint256)']],
    // TODO ['rocketUpgradeOneDotFour', 'function executed() view returns(bool)'],
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
    JSON.stringify(state, stringifier)
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

// allow for partial intervals
const actualEndTime = genesisTime + ((targetSlotEpoch + 1n) * slotsPerEpoch - 1n) * secondsPerSlot
log(1, `actualEndTime: ${actualEndTime}`)

async function checkSlotExists(slotNumber) {
  const path = `/eth/v1/beacon/headers/${slotNumber}`
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200 && response.status !== 404)
    console.warn(`Unexpected response status getting ${slotNumber} header: ${response.status}`)
  return response.status === 200
}
while (!(await checkSlotExists(targetBcSlot))) targetBcSlot--

log(1, `targetBcSlot: ${targetBcSlot}`)

async function getBlockNumberFromSlot(slotNumber) {
  const path = `/eth/v1/beacon/blinded_blocks/${slotNumber}`
  const key = `${path}/blockNumber`
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200) {
    console.warn(`Unexpected response status getting ${slotNumber} block: ${response.status}`)
    console.warn(`response text: ${await response.text()}`)
  }
  const json = await response.json()
  return BigInt(json.data.message.body.execution_payload_header.block_number)
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

const previousIntervalEventFilter = rocketRewardsPool.filters.RewardSnapshot(currentIndex - 1n)
const intervalBlocksApprox = intervalTime / 12n
const submissionDelayBlocksApprox = 400n
const foundEvents = await rocketRewardsPool.queryFilter(previousIntervalEventFilter,
  targetElBlock - intervalBlocksApprox,
  targetElBlock + submissionDelayBlocksApprox)
if (foundEvents.length !== 1)
  throw new Error(`Did not find exactly 1 RewardSnapshot event for Interval ${currentIndex - 1n}, got ${foundEvents.length}`)
const previousIntervalEvent = foundEvents.pop()
const RewardSubmission = previousIntervalEvent.args[1]
const ConsensusBlock = RewardSubmission[2]
const bnStartEpoch = ConsensusBlock / slotsPerEpoch + 1n
log(2, `bnStartEpoch: ${bnStartEpoch}`)
let bnStartBlock = slotsPerEpoch * bnStartEpoch
while (!(await checkSlotExists(bnStartBlock))) bnStartBlock++
log(1, `bnStartBlock: ${bnStartBlock}`)

const pendingRewards = BigInt(elState['rocketRewardsPool']['getPendingRPLRewards'][''])
const collateralPercent = BigInt(elState['rocketRewardsPool']['getClaimingContractPerc']['rocketClaimNode'])
const oDaoPercent = BigInt(elState['rocketRewardsPool']['getClaimingContractPerc']['rocketClaimTrustedNode'])
const pDaoPercent = BigInt(elState['rocketRewardsPool']['getClaimingContractPerc']['rocketClaimDAO'])

const _100Percent = oneEther
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

const ratio = BigInt(elState['rocketNetworkPrices']['getRPLPrice'][''])
const minCollateralFraction = BigInt(elState['rocketDAOProtocolSettingsNode']['getMinimumPerMinipoolStake'][''])
const maxCollateralFraction = 150n * 10n ** 16n

const allPubkeys = Array.from(new Set(Object.values(elState['rocketMinipoolManager']['getMinipoolPubkey'])))
log(3, `allPubkeys count: ${allPubkeys.length}`)
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
    if (response.status !== 200)
      throw new Error(`Unexpected response getting validator statuses: ${response.status}: ${await response.text()}`)
    const items = await response.json().then(j => j.data)
    for (const {validator: {pubkey, activation_epoch, exit_epoch}, index, status} of items)
      validatorStatuses[pubkey] = {activation_epoch, exit_epoch, index, status}
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

function getStakedRplValueInEth(nodeStake) {
  return nodeStake * ratio / oneEther
}

function getPercentOfBorrowedEth(stakedRplValueInEth, eligibleBorrowedEth) {
  return stakedRplValueInEth * oneHundredEther / eligibleBorrowedEth
}

function getNodeWeight(eligibleBorrowedEth, nodeStake) {
  if (currentIndex < rpip30Interval) return 0n
  if (!eligibleBorrowedEth) return 0n
  const stakedRplValueInEth = getStakedRplValueInEth(nodeStake)
  const percentOfBorrowedEth = getPercentOfBorrowedEth(stakedRplValueInEth, eligibleBorrowedEth)
  if (percentOfBorrowedEth <= fifteenEther)
    return 100n * stakedRplValueInEth
  else
    return ((thirteen6137Ether + 2n * ln(percentOfBorrowedEth - thirteenEther)) * eligibleBorrowedEth) / oneEther
}

function getSaturnZeroFee(baseFee, minipoolAddress) {
  const nodeAddress = elState[minipoolAddress]['getNodeAddress']
  const pubkey = elState['rocketMinipoolManager']['getMinipoolPubkey'][minipoolAddress]
  const nodeStake = BigInt(elState['rocketNodeStaking']['getNodeRPLStake'][nodeAddress])
  const {activation_epoch, exit_epoch} = validatorStatuses[pubkey]
  const eligibleBorrowedEth = getEligibility(activation_epoch, exit_epoch) ?
    BigInt(elState[minipoolAddress]['getUserDepositBalance']) : 0n
  const percentOfBorrowedEth = getPercentOfBorrowedEth(getStakedRplValueInEth(nodeStake), eligibleBorrowedEth)
  return max(baseFee,
    oneTenthEther +
    (oneFourHundredthEther * min(tenEther, percentOfBorrowedEth) / tenEther)
  )
}

function getMinipoolBondAndFee(minipoolAddress) {
  const rocketMinipoolBondReducer = elState['rocketMinipoolBondReducer']
  const currentBond = BigInt(elState[minipoolAddress]['getNodeDepositBalance'])
  const currentFee = BigInt(elState[minipoolAddress]['getNodeFee'])
  const previousBond = BigInt(rocketMinipoolBondReducer['getLastBondReductionPrevValue'][minipoolAddress])
  const previousFee = BigInt(rocketMinipoolBondReducer['getLastBondReductionPrevNodeFee'][minipoolAddress])
  const lastReduceTime = BigInt(rocketMinipoolBondReducer['getLastBondReductionTime'][minipoolAddress])
  const {bond, baseFee} = lastReduceTime > 0 && lastReduceTime > blockTime ?
    {bond: previousBond, baseFee: previousFee} :
    {bond: currentBond, baseFee: currentFee}
  const isEligibleInterval = true // TODO: change when rocketUpgradeOneDotFour executed is true
  const fee = bond < 16n * oneEther && isEligibleInterval ?
    getSaturnZeroFee(baseFee, minipoolAddress)
    : baseFee
  return {bond, fee, baseFee}
}

const nodeSmoothingTimes = new Map()
const eligiblePubkeysByNode = new Map()
const minipoolsByPubkey = new Map()

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
    if (!(pubkey in validatorStatuses)) continue
    const {activation_epoch, exit_epoch, status} = validatorStatuses[pubkey]
    if (getEligibility(activation_epoch, exit_epoch)) {
      eligibleBorrowedEth += BigInt(elState[minipoolAddress]['getUserDepositBalance'])
      eligibleBondedEth += BigInt(elState[minipoolAddress]['getNodeDepositBalance'])
    }
    const penaltyCount = BigInt(elState['rocketNetworkPenalties']['getPenaltyCount'][minipoolAddress])
    if (penaltyCount >= 3) {
      nodeSmoothingTimes.delete(nodeAddress)
      eligiblePubkeysByNode.delete(nodeAddress)
      log(3, `${nodeAddress} is a cheater`)
      continue
    }
    if (!nodeSmoothingTimes.has(nodeAddress))
      nodeSmoothingTimes.set(nodeAddress, {})
    if (!eligiblePubkeysByNode.has(nodeAddress))
      eligiblePubkeysByNode.set(nodeAddress, new Set())
    const notEligible = (
      ['pending_initialized', 'pending_queued'].includes(status) ||
      (activation_epoch == 'FAR_FUTURE_EPOCH' ||
       BigInt(activation_epoch) * slotsPerEpoch > targetBcSlot) ||
      (exit_epoch != 'FAR_FUTURE_EPOCH' &&
       BigInt(exit_epoch) * (slotsPerEpoch + 1n) < bnStartBlock)
    )
    if (!notEligible) {
      eligiblePubkeysByNode.get(nodeAddress).add(pubkey)
      minipoolsByPubkey.set(pubkey, minipoolAddress)
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

const rocketSmoothingPool = await getRocketAddress('rocketSmoothingPool', targetElBlock)
const smoothingPoolBalance = await provider.getBalance(rocketSmoothingPool, targetElBlock)
log(2, `smoothingPoolBalance: ${smoothingPoolBalance}`)

for (const [nodeAddress, smoothingTimes] of nodeSmoothingTimes.entries()) {
  const isOptedIn = elState['rocketNodeManager']['getSmoothingPoolRegistrationState'][nodeAddress]
  const statusChangeTime = BigInt(elState['rocketNodeManager']['getSmoothingPoolRegistrationChanged'][nodeAddress])
  smoothingTimes.optInTime = isOptedIn ? statusChangeTime : farPastTime
  smoothingTimes.optOutTime = isOptedIn ? farFutureTime : statusChangeTime
}

const eligiblePubkeysSet = new Set()
for (const s of eligiblePubkeysByNode.values())
  for (const pubkey of s.values())
    eligiblePubkeysSet.add(pubkey)
const eligiblePubkeys = Array.from(eligiblePubkeysSet)
log(3, `eligiblePubkeys count: ${eligiblePubkeys.length}`)
log(3, `eligiblePubkeys: ${eligiblePubkeys.slice(0, 5)}...`)

const EPOCHS_PER_QUERY = 32

log(3, `fetching attestations...`)
{
  let first_epoch = parseInt(bnStartEpoch)
  const options = {
    method: 'POST',
    headers: {'Content-Type': 'application/json'}
  }
  while (first_epoch < targetSlotEpoch) {
    const last_epoch = Math.min(first_epoch + EPOCHS_PER_QUERY, parseInt(targetSlotEpoch))
    log(3, `...${first_epoch}-${last_epoch}...`)
    const cacheFilename = `cache/a-${first_epoch}-${last_epoch}.json`
    if (!existsSync(cacheFilename)) {
      const attestations = {}
      options.body = JSON.stringify({ first_epoch, last_epoch, pubkeys: eligiblePubkeys })
      const response = await fetch(perfUrl, options)
      if (response.status !== 200)
        throw new Error(`Unexpected response getting attestations ${first_epoch}-${last_epoch}: ${response.status}: ${await response.text()}`)
      for (const [pubkey, epochs] of Object.entries(await response.json())) {
        if (!(pubkey in attestations)) attestations[pubkey] = {}
        Object.assign(attestations[pubkey], epochs)
      }
      writeFileSync(cacheFilename, JSON.stringify(attestations))
    }
    first_epoch = last_epoch + 1
  }
}
log(3, `fetched attestations`)

let totalMinipoolScore = 0n
let successfulAttestations = 0n
const minipoolPerformance = {}

log(3, `scoring attestations...`)
{
  let first_epoch = parseInt(bnStartEpoch)
  while (first_epoch < targetSlotEpoch) {
    const last_epoch = Math.min(first_epoch + EPOCHS_PER_QUERY, parseInt(targetSlotEpoch))
    log(3, `...${first_epoch}-${last_epoch}...`)
    const cacheFilename = `cache/p-${first_epoch}-${last_epoch}.json`
    let totalMinipoolScoreForRange = 0n
    let successfulAttestationsForRange = 0n
    const minipoolPerformanceForRange = {}
    if (existsSync(cacheFilename)) {
      const {total, attestations, performance} = JSON.parse(readFileSync(cacheFilename))
      totalMinipoolScoreForRange = BigInt(total)
      successfulAttestationsForRange = BigInt(attestations)
      for (const [minipoolAddress, {score, successes, missing}] of Object.entries(performance)) {
        minipoolPerformanceForRange[minipoolAddress] = {
          score: BigInt(score), successes, missing
        }
      }
    }
    else {
      const attestationCacheFilename = `cache/a-${first_epoch}-${last_epoch}.json`
      const attestations = JSON.parse(readFileSync(attestationCacheFilename))
      for (const [pubkey, epochs] of Object.entries(attestations)) {
        const minipoolAddress = minipoolsByPubkey.get(pubkey)
        const nodeAddress = elState[minipoolAddress]['getNodeAddress']
        const {optInTime, optOutTime} = nodeSmoothingTimes.get(nodeAddress)
        for (const [epoch, {slot, attested_slot}] of Object.entries(epochs)) {
          const slotIndex = BigInt(slot)
          if (slotIndex < bnStartBlock) continue
          const blockTime = genesisTime + secondsPerSlot * slotIndex
          if (blockTime < optInTime || blockTime > optOutTime) continue
          const statusTime = BigInt(elState[minipoolAddress]['getStatusTime'])
          if (blockTime < statusTime) continue
          if (!(minipoolAddress in minipoolPerformanceForRange))
            minipoolPerformanceForRange[minipoolAddress] = {
              score: 0n, successes: 0, missing: []
            }
          if (!attested_slot || attested_slot > slotIndex + slotsPerEpoch) {
            minipoolPerformanceForRange[minipoolAddress].missing.push(slot)
            continue
          }
          const {bond, fee} = getMinipoolBondAndFee(minipoolAddress)
          const minipoolScore = (BigInt(1e18) - fee) * bond / BigInt(32e18) + fee
          minipoolPerformanceForRange[minipoolAddress].score += minipoolScore
          minipoolPerformanceForRange[minipoolAddress].successes += 1
          totalMinipoolScoreForRange += minipoolScore
          successfulAttestationsForRange += 1n
        }
      }
      const data = {
        total: totalMinipoolScoreForRange,
        attestations: successfulAttestationsForRange,
        performance: minipoolPerformanceForRange
      }
      writeFileSync(cacheFilename, JSON.stringify(data, stringifier))
    }
    totalMinipoolScore += totalMinipoolScoreForRange
    successfulAttestations += successfulAttestationsForRange
    for (const [minipoolAddress, {score, successes, missing}] of Object.entries(minipoolPerformanceForRange)) {
      if (!(minipoolAddress in minipoolPerformance))
        minipoolPerformance[minipoolAddress] = {
          attestationScore: 0n,
          successfulAttestations: 0,
          missingAttestationSlots: []
        }
      minipoolPerformance[minipoolAddress].attestationScore += score
      minipoolPerformance[minipoolAddress].successfulAttestations += successes
      minipoolPerformance[minipoolAddress].missingAttestationSlots.push(...missing)
    }
    first_epoch = last_epoch + 1
  }
}
log(3, `scored attestations`)
log(2, `successfulAttestations: ${successfulAttestations}`)
log(2, `totalMinipoolScore: ${totalMinipoolScore}`)
writeFileSync('minipool-performance.json', JSON.stringify(minipoolPerformance, stringifier))

const nodeRewards = new Map()
function addNodeReward(nodeAddress, token, amount) {
  if (!amount) return
  if (!nodeRewards.has(nodeAddress)) nodeRewards.set(nodeAddress,
    {smoothing_pool_eth: 0n, collateral_rpl: 0n, oracle_dao_rpl: 0n}
  )
  nodeRewards.get(nodeAddress)[token] += amount
}
nodeCollateralAmounts.forEach((v, k) => addNodeReward(k, 'collateral_rpl', v))
oDaoAmounts.forEach((v, k) => addNodeReward(k, 'oracle_dao_rpl', v))

const totalNodeOpShare = smoothingPoolBalance * totalMinipoolScore / (successfulAttestations * _100Percent)
let totalEthForMinipools = 0n

log(2, `totalNodeOpShare: ${totalNodeOpShare}`)

for (const [minipoolAddress, {attestationScore}] of Object.entries(minipoolPerformance)) {
  const minipoolEth = totalNodeOpShare * attestationScore / totalMinipoolScore
  addNodeReward(elState[minipoolAddress]['getNodeAddress'], 'smoothing_pool_eth', minipoolEth)
  totalEthForMinipools += minipoolEth
}

const bonusWindowsByMinipool = {}
const minipoolsByBonusStart = {}
const minipoolsByBonusEnd = {}
const minipoolWithdrawals = {}
const minipoolBalances = {}
let minBonusWindowStart = targetBcSlot + slotsPerEpoch
let maxBonusWindowEnd = 0n
for (const [pubkey, minipoolAddress] of minipoolsByPubkey.entries()) {
  minipoolWithdrawals[minipoolAddress] = 0n
  minipoolBalances[minipoolAddress] = {}
  const nodeAddress = elState[minipoolAddress]['getNodeAddress']
  const statusTime = BigInt(elState[minipoolAddress]['getStatusTime'])
  const {optInTime, optOutTime} = nodeSmoothingTimes.get(nodeAddress)
  const rocketMinipoolBondReducer = elState['rocketMinipoolBondReducer']
  const lastReduceTime = BigInt(rocketMinipoolBondReducer['getLastBondReductionTime'][minipoolAddress])
  const eligibleStartTime = max(startTime, max(statusTime, max(optInTime, lastReduceTime)))
  const eligibleEndTime = min(actualEndTime, optOutTime)
  const rewardStartBcSlot = (eligibleStartTime - genesisTime + (secondsPerSlot - 1n)) / secondsPerSlot
  const rewardEndBcSlot = (eligibleEndTime - genesisTime + (secondsPerSlot - 1n)) / secondsPerSlot
  bonusWindowsByMinipool[minipoolAddress] = {rewardStartBcSlot, rewardEndBcSlot}
  const startKey = rewardStartBcSlot.toString()
  const endKey = rewardEndBcSlot.toString()
  minipoolsByBonusStart[startKey] ||= []
  minipoolsByBonusEnd[endKey] ||= []
  const mp = {pubkey, minipoolAddress}
  minipoolsByBonusStart[startKey].push(mp)
  minipoolsByBonusEnd[endKey].push(mp)
  if (rewardStartBcSlot < minBonusWindowStart)
    minBonusWindowStart = rewardStartBcSlot
  if (maxBonusWindowEnd < rewardEndBcSlot)
    maxBonusWindowEnd = rewardEndBcSlot
}

log(3, `fetching start and end balances...`)
// TODO: cache?
{
  const path = slot => `/eth/v1/beacon/states/${slot}/validator_balances`
  const options = pubkeys => ({
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(pubkeys)
  })
  async function getBalance(mpsByKey, key) {
    for (const [bonusSlot, mps] of Object.entries(mpsByKey)) {
      log(3, `processing ${mps.length} MPs for ${key} balance slot ${bonusSlot}...`)
      const cacheFilename = `cache/b-${key}-${bonusSlot}.json`
      let beaconData
      try {
        beaconData = JSON.parse(readFileSync(cacheFilename))
      }
      catch {
        const url = new URL(`${beaconRpcUrl}${path(bonusSlot)}`)
        const response = await fetch(url, options(mps.map(({pubkey}) => pubkey)))
        if (response.status !== 200)
          console.warn(`Unexpected response status getting ${bonusSlot} ${key} balances: ${response.status}`)
        const { data } = await response.json()
        beaconData = data
        writeFileSync(cacheFilename, JSON.stringify(data))
      }
      const addressByIndex = {}
      for (const {pubkey, minipoolAddress} of mps)
        addressByIndex[validatorStatuses[pubkey].index] = minipoolAddress
      for (const {index, balance} of beaconData) {
        minipoolBalances[addressByIndex[index]][key] = BigInt(balance) * oneGwei
      }
    }
  }
  await getBalance(minipoolsByBonusStart, 'start')
  await getBalance(minipoolsByBonusEnd, 'end')
}

log(3, `fetching withdrawals from ${minBonusWindowStart} to ${maxBonusWindowEnd}...`)
{
  let firstSlot = minBonusWindowStart
  while (firstSlot < maxBonusWindowEnd) {
    const pastLastSlot = min(firstSlot + BigInt(EPOCHS_PER_QUERY) * slotsPerEpoch, maxBonusWindowEnd)
    const minipoolWithdrawalsForRange = {}
    const cacheFilename = `cache/w-${firstSlot}-${pastLastSlot}.json`
    try {
      Object.assign(minipoolWithdrawalsForRange, JSON.parse(readFileSync(cacheFilename)))
    }
    catch {
      let slot = firstSlot
      while (slot < pastLastSlot) {
        if (slot % 100n == 0n) log(3, `up to ${slot}...`)
        const path = `/eth/v2/beacon/blocks/${slot}`
        const url = new URL(`${beaconRpcUrl}${path}`)
        const response = await fetch(url)
        if (response.status !== 200 && response.status !== 404)
          throw new Error(`Unexpected response getting withdrawals for ${slot}: ${response.status}: ${await response.text()}`)
        const slotWithdrawals = response.status === 404 ? [] :
          await response.json().then(j => j.data.message.body.execution_payload.withdrawals)
        for (const {address, amount} of slotWithdrawals) {
          const {rewardStartBcSlot, rewardEndBcSlot} = bonusWindowsByMinipool[address] || {rewardEndBcSlot: 0n}
          if (rewardStartBcSlot <= slot && slot < rewardEndBcSlot) {
            minipoolWithdrawalsForRange[address] ||= 0n
            minipoolWithdrawalsForRange[address] += BigInt(amount) * oneGwei
          }
        }
        slot += 1n
      }
      for (const [minipoolAddress, withdrawn] of Object.entries(minipoolWithdrawalsForRange)) {
        minipoolWithdrawalsForRange[minipoolAddress] = withdrawn.toString()
      }
      writeFileSync(cacheFilename, JSON.stringify(minipoolWithdrawalsForRange))
    }
    for (const [minipoolAddress, withdrawn] of Object.entries(minipoolWithdrawalsForRange)) {
      minipoolWithdrawals[minipoolAddress] += BigInt(withdrawn)
    }
    firstSlot = pastLastSlot
  }
}

const nodeBonus = {}
let totalConsensusBonus = 0n
for (const minipoolAddress of minipoolsByPubkey.values()) {
  const withdrawn = minipoolWithdrawals[minipoolAddress]
  const {end: endBalance, start: startBalance} = minipoolBalances[minipoolAddress]
  const {bond, fee, baseFee} = getMinipoolBondAndFee(minipoolAddress)
  const consensusIncome = endBalance + withdrawn - max(thirtyTwoEther, startBalance)
  const bonusShare = (fee - baseFee) * (thirtyTwoEther - bond) / thirtyTwoEther
  const minipoolBonus = max(0n, consensusIncome * bonusShare / oneEther)
  const nodeAddress = elState[minipoolAddress]['getNodeAddress']
  nodeBonus[nodeAddress] ||= 0n
  nodeBonus[nodeAddress] += minipoolBonus
  totalConsensusBonus += minipoolBonus
}

const remainingBalance = smoothingPoolBalance - totalEthForMinipools
if (totalConsensusBonus > remainingBalance) {
  for (const [nodeAddress, rawBonus] of Object.entries(nodeBonus))
    nodeBonus[nodeAddress] = rawBonus * remainingBalance / totalConsensusBonus
}

for (const [nodeAddress, bonus] of Object.entries(nodeBonus)) {
  addNodeReward(nodeAddress, 'smoothing_pool_eth', bonus)
  totalEthForMinipools += bonus
}

log(2, `totalEthForMinipools: ${totalEthForMinipools}`)

function nodeMetadataHash(nodeAddress, totalRPL, totalETH) {
  const data = new Uint8Array(20 + 32 + 32 + 32)
  data.set(ethers.getBytes(nodeAddress), 0)
  data.fill(0, 20, 20 + 32)
  const RPLuint8s = ethers.toBeArray(totalRPL)
  const ETHuint8s = ethers.toBeArray(totalETH)
  data.set(RPLuint8s, 20 + 32 + (32 - RPLuint8s.length))
  data.set(ETHuint8s, 20 + 32 + 32 + (32 - ETHuint8s.length))
  return ethers.keccak256(data)
}

const nodeRewardsObject = {}
const nodeHashes = new Map()
nodeRewards.forEach(({smoothing_pool_eth, collateral_rpl, oracle_dao_rpl}, nodeAddress) => {
  if (0 < smoothing_pool_eth || 0 < collateral_rpl || 0 < oracle_dao_rpl) {
    nodeHashes.set(nodeAddress, nodeMetadataHash(nodeAddress, collateral_rpl + oracle_dao_rpl, smoothing_pool_eth))
    nodeRewardsObject[nodeAddress] = {ETH: smoothing_pool_eth.toString(),
                                      RPL: (collateral_rpl + oracle_dao_rpl).toString()}
  }
})
writeFileSync('node-rewards.json', JSON.stringify(nodeRewardsObject))

const nullHash = ethers.hexlify(new Uint8Array(32))
const leafValues = Array.from(nodeHashes.values()).sort()
const rowHashes = leafValues.concat(
  Array(Math.pow(2, Math.ceil(Math.log2(leafValues.length)))
        - leafValues.length).fill(nullHash))
log(3, `number of leaves: ${leafValues.length} (${rowHashes.length} with nulls)`)
while (rowHashes.length > 1) {
  let i = 0
  while (i < rowHashes.length) {
    const [left, right] = rowHashes.slice(i, i + 2).sort()
    const branch = new Uint8Array(64)
    branch.set(ethers.getBytes(left), 0)
    branch.set(ethers.getBytes(right), 32)
    rowHashes.splice(i++, 2, ethers.keccak256(branch))
  }
}
log(1, `merkle root: ${rowHashes}`)
rowHashes.push(`interval: ${currentIndex}`)
rowHashes.push(`start epoch: ${bnStartEpoch}`)
rowHashes.push(`target epoch: ${targetSlotEpoch}`)
writeFileSync('merkle-root.txt', rowHashes.join('\n'))

let consensus_start_block = parseInt(bnStartEpoch * slotsPerEpoch)
while (!(await checkSlotExists(consensus_start_block)))
  consensus_start_block++

const sszFileName = `rp-rewards-mainnet-${currentIndex}.ssz`
log(2, `Generating ssz file ${sszFileName}...`)

const sszFile = {
  magic: new Uint8Array([0x52, 0x50, 0x52, 0x54]),
  rewards_file_version: 3,
  ruleset_version: 9,
  network: 1,
  index: currentIndex,
  start_time: genesisTime + bnStartEpoch * slotsPerEpoch * secondsPerSlot,
  end_time: actualEndTime,
  consensus_start_block,
  consensus_end_block: targetBcSlot,
  execution_start_block: await getBlockNumberFromSlot(consensus_start_block),
  execution_end_block: targetElBlock,
  intervals_passed: tryBigInt(process.env.OVERRIDE_TARGET_EPOCH) ? 0 : 1,
  merkle_root: Buffer.from(rowHashes[0].slice(2), 'hex'),
  total_rewards: {
    protocol_dao_rpl: actualPDaoRewards,
    total_collateral_rpl: totalCalculatedCollateralRewards,
    total_oracle_dao_rpl: totalCalculatedODaoRewards,
    total_smoothing_pool_eth: smoothingPoolBalance,
    pool_staker_smoothing_pool_eth: smoothingPoolBalance - totalEthForMinipools,
    node_operator_smoothing_pool_eth: totalEthForMinipools,
    total_node_weight: totalNodeWeight
  },
  network_rewards: [{
    network: 0,
    collateral_rpl: totalCalculatedCollateralRewards,
    oracle_dao_rpl: totalCalculatedODaoRewards,
    smoothing_pool_eth: totalEthForMinipools
  }],
  node_rewards:
    Array.from(nodeRewards.entries())
    .map(([k, v]) => [Buffer.from(k.slice(2), 'hex'), v])
    .sort(([k1], [k2]) => Buffer.compare(k1, k2))
    .map(([address, {collateral_rpl, oracle_dao_rpl, smoothing_pool_eth}]) =>
      ({address, network: 0, collateral_rpl, oracle_dao_rpl, smoothing_pool_eth})
    )
}

const serializeUint = (n, nbits) => Buffer.from(n.toString(16).padStart(2 * nbits / 8, '0'), 'hex').reverse()

const serializationPieces = [
  sszFile.magic,
  serializeUint(sszFile.rewards_file_version,  64),
  serializeUint(sszFile.ruleset_version,       64),
  serializeUint(sszFile.network,               64),
  serializeUint(sszFile.index,                 64),
  serializeUint(sszFile.start_time,            64),
  serializeUint(sszFile.end_time,              64),
  serializeUint(sszFile.consensus_start_block, 64),
  serializeUint(sszFile.consensus_end_block,   64),
  serializeUint(sszFile.execution_start_block, 64),
  serializeUint(sszFile.execution_end_block,   64),
  serializeUint(sszFile.intervals_passed,      64),
  sszFile.merkle_root,
  serializeUint(sszFile.total_rewards.protocol_dao_rpl,                 256),
  serializeUint(sszFile.total_rewards.total_collateral_rpl,             256),
  serializeUint(sszFile.total_rewards.total_oracle_dao_rpl,             256),
  serializeUint(sszFile.total_rewards.total_smoothing_pool_eth,         256),
  serializeUint(sszFile.total_rewards.pool_staker_smoothing_pool_eth,   256),
  serializeUint(sszFile.total_rewards.node_operator_smoothing_pool_eth, 256),
  serializeUint(sszFile.total_rewards.total_node_weight,                256),
]
const fixedLength = serializationPieces.reduce((n, a) => n + a.length, 0) + 4 + 4
serializationPieces.push(serializeUint(fixedLength, 32))
const networkRewardLength = (64 + 3 * 256) / 8
serializationPieces.push(serializeUint(fixedLength + networkRewardLength, 32))
serializationPieces.push(
  serializeUint(sszFile.network_rewards[0].network, 64),
  serializeUint(sszFile.network_rewards[0].collateral_rpl,     256),
  serializeUint(sszFile.network_rewards[0].oracle_dao_rpl,     256),
  serializeUint(sszFile.network_rewards[0].smoothing_pool_eth, 256)
)
for (const {address, network, collateral_rpl, oracle_dao_rpl, smoothing_pool_eth} of sszFile.node_rewards) {
  serializationPieces.push(
    address,
    serializeUint(network, 64),
    serializeUint(collateral_rpl,     256),
    serializeUint(oracle_dao_rpl,     256),
    serializeUint(smoothing_pool_eth, 256)
  )
}

const ssz = Buffer.concat(serializationPieces)
writeFileSync(sszFileName, ssz)
log(2, `ssz file of ${ssz.length} bytes written`)
