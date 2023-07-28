import 'dotenv/config'
import { ethers } from 'ethers'
import { Worker, MessageChannel } from 'node:worker_threads'
import { provider, startBlock, slotsPerEpoch, networkName, stakingStatus, tryBigInt, makeLock,
         log, cacheUserPort, socketCall, cachedCall } from './lib.js'

const currentIndex = BigInt(await cachedCall('rocketRewardsPool', 'getRewardIndex', [], 'targetElBlock'))
log(2, `currentIndex: ${currentIndex}`)

const pendingRewards = BigInt(await cachedCall(
  'rocketRewardsPool', 'getPendingRPLRewards', [], 'targetElBlock'))
const collateralPercent = BigInt(await cachedCall(
  'rocketRewardsPool', 'getClaimingContractPerc', ['rocketClaimNode'], 'targetElBlock'))
const oDaoPercent = BigInt(await cachedCall(
  'rocketRewardsPool', 'getClaimingContractPerc', ['rocketClaimTrustedNode'], 'targetElBlock'))
const pDaoPercent = BigInt(await cachedCall(
  'rocketRewardsPool', 'getClaimingContractPerc', ['rocketClaimDAO'], 'targetElBlock'))

const _100Percent = ethers.parseEther('1')
const collateralRewards = pendingRewards * collateralPercent / _100Percent
const oDaoRewards = pendingRewards * oDaoPercent / _100Percent
const pDaoRewards = pendingRewards * pDaoPercent / _100Percent
log(2, `pendingRewards: ${pendingRewards}`)
log(2, `collateralRewards: ${collateralRewards}`)
log(2, `oDaoRewards: ${oDaoRewards}`)
log(2, `pDaoRewards: ${pDaoRewards}`)

const nodeCount = BigInt(await cachedCall('rocketNodeManager', 'getNodeCount', [], 'targetElBlock'))
log(2, `nodeCount: ${nodeCount}`)
const nodeIndices = Array.from(Array(parseInt(nodeCount)).keys())
const nodeAddresses = await Promise.all(
  nodeIndices.map(i => cachedCall('rocketNodeManager', 'getNodeAt', [i.toString()], 'targetElBlock'))
)
log(3, `nodeAddresses: ${nodeAddresses.slice(0, 5)}...`)

const ratio = BigInt(await cachedCall('rocketNetworkPrices', 'getRPLPrice', [], 'targetElBlock'))
const minCollateralFraction = BigInt(await cachedCall(
  'rocketDAOProtocolSettingsNode', 'getMinimumPerMinipoolStake', [], 'targetElBlock'))
const maxCollateralFraction = BigInt(await cachedCall(
  'rocketDAOProtocolSettingsNode', 'getMaximumPerMinipoolStake', [], 'targetElBlock'))

let totalEffectiveRplStake = 0n

const nodeEffectiveStakes = new Map()

const MAX_CONCURRENT_MINIPOOLS = parseInt(process.env.MAX_CONCURRENT_MINIPOOLS) || 10
const MAX_CONCURRENT_NODES = parseInt(process.env.MAX_CONCURRENT_NODES) || 10

const targetElBlockTimestamp = await socketCall(['targetElBlockTimestamp'])
const intervalTime = await socketCall(['intervalTime'])
const targetSlotEpoch = await socketCall(['targetSlotEpoch'])

async function processNodeRPL(i) {
  const nodeAddress = nodeAddresses[i]
  const minipoolCount = BigInt(await cachedCall(
    'rocketMinipoolManager', 'getNodeMinipoolCount', [nodeAddress], 'targetElBlock'))
  log(3, `Processing ${nodeAddress}'s ${minipoolCount} minipools`)
  let eligibleBorrowedEth = 0n
  let eligibleBondedEth = 0n
  async function processMinipool(minipoolAddress) {
    const minipoolStatus = parseInt(await cachedCall(minipoolAddress, 'getStatus', [], 'targetElBlock'))
    if (minipoolStatus != stakingStatus) return
    const pubkey = await cachedCall(
      'rocketMinipoolManager', 'getMinipoolPubkey', [minipoolAddress], 'finalized')
    const validatorStatus = await socketCall(['beacon', 'getValidatorStatus', pubkey])
    const activationEpoch = validatorStatus.activation_epoch
    const exitEpoch = validatorStatus.exit_epoch
    const eligible = activationEpoch != 'FAR_FUTURE_EPOCH' && BigInt(activationEpoch) < targetSlotEpoch &&
                     (exitEpoch == 'FAR_FUTURE_EPOCH' || targetSlotEpoch < BigInt(exitEpoch))
    if (eligible) {
      const borrowedEth = BigInt(await cachedCall(minipoolAddress, 'getUserDepositBalance', [], 'targetElBlock'))
      eligibleBorrowedEth += borrowedEth
      const bondedEth = BigInt(await cachedCall(minipoolAddress, 'getNodeDepositBalance', [], 'targetElBlock'))
      eligibleBondedEth += bondedEth
    }
  }
  const minipoolIndicesToProcess = Array.from(Array(parseInt(minipoolCount)).keys())
  while (minipoolIndicesToProcess.length) {
    log(4, `${minipoolIndicesToProcess.length} minipools left for ${nodeAddress}`)
    await Promise.all(
      minipoolIndicesToProcess.splice(0, MAX_CONCURRENT_MINIPOOLS)
      .map(i => cachedCall('rocketMinipoolManager', 'getNodeMinipoolAt', [nodeAddress, i], 'finalized')
                .then(addr => processMinipool(addr)))
    )
  }
  const minCollateral = eligibleBorrowedEth * minCollateralFraction / ratio
  const maxCollateral = eligibleBondedEth * maxCollateralFraction / ratio
  const nodeStake = BigInt(await cachedCall(
    'rocketNodeStaking', 'getNodeRPLStake', [nodeAddress], 'targetElBlock')
  )
  let nodeEffectiveStake = nodeStake < minCollateral ? 0n :
                           nodeStake < maxCollateral ? nodeStake : maxCollateral
  const registrationTime = BigInt(await cachedCall(
    'rocketNodeManager', 'getNodeRegistrationTime', [nodeAddress], 'finalized'))
  const nodeAge = targetElBlockTimestamp - registrationTime
  if (nodeAge < intervalTime)
    nodeEffectiveStake = nodeEffectiveStake * nodeAge / intervalTime
  log(3, `${nodeAddress} effective stake: ${nodeEffectiveStake}`)
  nodeEffectiveStakes.set(nodeAddress, nodeEffectiveStake)
  totalEffectiveRplStake += nodeEffectiveStake
}

const numberOfMinipools = BigInt(
  await cachedCall('rocketMinipoolManager', 'getMinipoolCount', [], 'targetElBlock'))

const nodeCollateralAmounts = new Map()
let totalCalculatedCollateralRewards = 0n

const nodeIndicesToProcessRPL = nodeIndices.slice()
while (nodeIndicesToProcessRPL.length) {
  log(3, `${nodeIndicesToProcessRPL.length} nodes left to process RPL`)
  await Promise.all(
    nodeIndicesToProcessRPL.splice(0, MAX_CONCURRENT_NODES)
    .map(i => processNodeRPL(i))
  )
}
log(1, `totalEffectiveRplStake: ${totalEffectiveRplStake}`)


for (const nodeAddress of nodeAddresses) {
  const nodeEffectiveStake = nodeEffectiveStakes.get(nodeAddress)
  const nodeCollateralAmount = collateralRewards * nodeEffectiveStake / totalEffectiveRplStake
  nodeCollateralAmounts.set(nodeAddress, nodeCollateralAmount)
  totalCalculatedCollateralRewards += nodeCollateralAmount
}

log(1, `totalCalculatedCollateralRewards: ${totalCalculatedCollateralRewards}`)
if (collateralRewards - totalCalculatedCollateralRewards > numberOfMinipools)
  throw new Error('collateral calculation has excessive error')

const oDaoCount = BigInt(await cachedCall('rocketDAONodeTrusted', 'getMemberCount', [], 'targetElBlock'))
const oDaoIndices = Array.from(Array(parseInt(oDaoCount)).keys())
const oDaoAddresses = await Promise.all(
  oDaoIndices.map(i => cachedCall('rocketDAONodeTrusted', 'getMemberAt', [i.toString()], 'targetElBlock'))
)
log(3, `oDaoAddresses: ${oDaoAddresses.slice(0, 5)}...`)

let totalParticipatedSeconds = 0n
const oDaoParticipatedSeconds = new Map()
const oDaoIndicesToProcess = oDaoIndices.slice()
while (oDaoIndicesToProcess.length) {
  log(3, `${oDaoIndicesToProcess.length} oDAO nodes left to process`)
  await Promise.all(oDaoIndicesToProcess.splice(0, MAX_CONCURRENT_NODES)
    .map(async i => {
      const nodeAddress = oDaoAddresses[i]
      const joinTime = BigInt(await cachedCall(
        'rocketDAONodeTrusted', 'getMemberJoinedTime', [nodeAddress], 'finalized'))
      const odaoTime = targetElBlockTimestamp - joinTime
      const participatedSeconds = odaoTime < intervalTime ? odaoTime : intervalTime
      oDaoParticipatedSeconds.set(nodeAddress, participatedSeconds)
      totalParticipatedSeconds += participatedSeconds
    })
  )
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


if (currentIndex == 0) process.exit()

const ExecutionBlock = await socketCall(['ExecutionBlock'])
const ConsensusBlock = await socketCall(['ConsensusBlock'])

const bnStartEpoch = tryBigInt(process.env.OVERRIDE_START_EPOCH) || ConsensusBlock / slotsPerEpoch + 1n
log(2, `bnStartEpoch: ${bnStartEpoch}`)

const possiblyEligibleMinipoolIndexArray = new BigUint64Array(
  new SharedArrayBuffer(
    BigUint64Array.BYTES_PER_ELEMENT * (1 + (1 + 3) * parseInt(numberOfMinipools))
  )
)

const NUM_WORKERS = parseInt(process.env.NUM_WORKERS) || 12
const workerPorts = new Map()

const makeWorkers = (path, workerData) =>
  Array.from(Array(NUM_WORKERS).keys()).map(i => {
    const { port1, port2 } = new MessageChannel()
    port1.on('message', msg => cacheUserPort.postMessage(msg))
    const data = {
      worker: new Worker(path, {workerData: {cacheUserPort: port2, value: workerData}, transferList: [port2]}),
      promise: i,
      resolveWhenReady: null
    }
    workerPorts.set(data.worker.threadId, port1)
    data.worker.on('message', (msg) => {
      if (msg === "done") {
        if (typeof data.resolveWhenReady == 'function')
          data.resolveWhenReady(i)
      }
    })
    return data
  })

cacheUserPort.on('message', msg => {
  if (typeof msg.id == 'object' && workerPorts.has(msg.id.threadId))
    workerPorts.get(msg.id.threadId).postMessage(msg)
})

const smoothingWorkers = makeWorkers('./smoothing.js', possiblyEligibleMinipoolIndexArray)

async function getWorker(workers) {
  const i = await Promise.any(workers.map(data => data.promise))
  workers[i].promise = new Promise(resolve => {
    workers[i].resolveWhenReady = resolve
  })
  return workers[i].worker
}

for (const i of nodeIndices) {
  const left = nodeIndices.length - i
  if (left % 10 == 0)
    log(3, `${left} nodes left to process smoothing times`)
  const nodeAddress = nodeAddresses[i]
  const worker = await getWorker(smoothingWorkers)
  worker.postMessage({i, nodeAddress})
}

await Promise.all(smoothingWorkers.map(data => data.promise))
smoothingWorkers.forEach(data => data.worker.postMessage('exit'))

log(3, `${possiblyEligibleMinipoolIndexArray[0]} eligible minipools`)

const dutiesWorkers = makeWorkers('./duties.js', possiblyEligibleMinipoolIndexArray)

const intervalEpochsToGetDuties = Array.from(
  Array(parseInt(targetSlotEpoch - bnStartEpoch + 1n)).keys())
.map(i => bnStartEpoch + BigInt(i))

const timestamp = () => Intl.DateTimeFormat('en-GB',
  {hour: 'numeric', minute: 'numeric', second: 'numeric'})
  .format(new Date())

while (intervalEpochsToGetDuties.length) {
  if (intervalEpochsToGetDuties.length % 10 == 0)
    log(3, `${timestamp()}: ${intervalEpochsToGetDuties.length} epochs left to get duties`)
  const epochIndex = intervalEpochsToGetDuties.shift().toString()
  if (await socketCall(['duties', epochIndex, 'check'])) continue
  const worker = await getWorker(dutiesWorkers)
  worker.postMessage(epochIndex)
}

await Promise.all(dutiesWorkers.map(data => data.promise))
dutiesWorkers.forEach(data => data.worker.postMessage('exit'))

const minipoolScores = new Map()
const minipoolAttestations = new Map()
const minipoolAttestationsLock = makeLock()
const minipoolScoresLock = makeLock()
let totalMinipoolScore = 0n
let successfulAttestations = 0n

const attestationWorkers = makeWorkers('./attestations.js')
const scoresWorkers = makeWorkers('./scores.js')

async function processAttestation ({minipoolAddress, slotIndex}) {
  if (typeof minipoolAddress != 'string' || typeof slotIndex != 'number') return
  if (await minipoolAttestationsLock(() => {
    if (!minipoolAttestations.has(minipoolAddress)) minipoolAttestations.set(minipoolAddress, new Set())
    const slots = minipoolAttestations.get(minipoolAddress)
    if (slots.has(slotIndex)) return true
    slots.add(slotIndex)
    successfulAttestations++
  })) return
  const worker = await getWorker(scoresWorkers)
  worker.postMessage({minipoolAddress, slotIndex})
}

attestationWorkers.forEach(data => data.worker.on('message', processAttestation))

function processScore ({minipoolAddress, minipoolScore}) {
  if (typeof minipoolAddress != 'string' || typeof minipoolScore != 'bigint') return
  return minipoolScoresLock(() => {
    minipoolScores.set(minipoolAddress,
      minipoolScores.has(minipoolAddress) ?
      minipoolScores.get(minipoolAddress) + minipoolScore :
      minipoolScore)
    totalMinipoolScore += minipoolScore
  })
}

scoresWorkers.forEach(data => data.worker.on('message', processScore))

const epochs = Array.from(
  Array(parseInt(targetSlotEpoch + 1n - bnStartEpoch + 1n)).keys())
.map(i => bnStartEpoch + BigInt(i))

while (epochs.length) {
  if (epochs.length % 10 == 0)
    log(3, `${timestamp()}: ${epochs.length} epochs left to process attestations`)
  const epoch = epochs.shift().toString()
  const worker = await getWorker(attestationWorkers)
  worker.postMessage(epoch)
}

await Promise.all(attestationWorkers.map(data => data.promise))
attestationWorkers.forEach(data => data.worker.postMessage('exit'))

await Promise.all(scoresWorkers.map(data => data.promise))
scoresWorkers.forEach(data => data.worker.postMessage('exit'))

log(3, `successfulAttestations: ${successfulAttestations}`)
log(3, `totalMinipoolScore: ${totalMinipoolScore}`)

const nodeRewards = new Map()
function addNodeReward(nodeAddress, token, amount) {
  if (!amount) return
  if (!nodeRewards.has(nodeAddress)) nodeRewards.set(nodeAddress, {ETH: 0n, RPL: 0n})
  nodeRewards.get(nodeAddress)[token] += amount
}
nodeCollateralAmounts.forEach((v, k) => addNodeReward(k, 'RPL', v))
oDaoAmounts.forEach((v, k) => addNodeReward(k, 'RPL', v))

const smoothingPoolBalance = await socketCall(['smoothingPoolBalance'])
const totalNodeOpShare = smoothingPoolBalance * totalMinipoolScore / (successfulAttestations * _100Percent)
let totalEthForMinipools = 0n

log(3, `totalNodeOpShare: ${totalNodeOpShare}`)

for (const [minipoolAddress, minipoolScore] of minipoolScores.entries()) {
  const minipoolEth = totalNodeOpShare * minipoolScore / totalMinipoolScore
  addNodeReward(await cachedCall(minipoolAddress, 'getNodeAddress', [], 'finalized'), 'ETH', minipoolEth)
  totalEthForMinipools += minipoolEth
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
nodeRewards.forEach(({ETH, RPL}, nodeAddress) => {
  if (0 < ETH || 0 < RPL) {
    nodeHashes.set(nodeAddress, nodeMetadataHash(nodeAddress, RPL, ETH))
    nodeRewardsObject[nodeAddress] = {ETH, RPL}
  }
})
import { writeFileSync } from 'node:fs'
writeFileSync('node-rewards.json',
  JSON.stringify(nodeRewardsObject,
    (key, value) => typeof value === 'bigint' ? value.toString() : value))

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
