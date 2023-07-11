import 'dotenv/config'
import { ethers } from 'ethers'
import { provider, startBlock, slotsPerEpoch, networkName, stakingStatus, tryBigInt, makeLock,
         log, addressToUint64s, uint64sTo256, uint64sToAddress, socketCall, cachedCall } from './lib.js'

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

const targetElBlockTimestamp = BigInt(await socketCall(['targetElBlockTimestamp']))
const intervalTime = BigInt(await socketCall(['intervalTime']))
const targetSlotEpoch = BigInt(await socketCall(['targetSlotEpoch']))

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
    const validatorStatus = JSON.parse(await socketCall(['beacon', 'getValidatorStatus', pubkey]))
    const activationEpoch = validatorStatus.activation_epoch
    const exitEpoch = validatorStatus.exit_epoch
    const eligible = activationEpoch != 'FAR_FUTURE_EPOCH' && BigInt(activationEpoch) < targetSlotEpoch &&
                     (exitEpoch == 'FAR_FUTURE_EPOCH' || targetSlotEpoch < BigInt(exitEpoch))
    if (eligible) {
      const borrowedEth = BigInt(await cachedCall(minipoolAddress, 'getUserDepositBalance', [], 'finalized'))
      eligibleBorrowedEth += borrowedEth
      const bondedEth = BigInt(await cachedCall(minipoolAddress, 'getNodeDepositBalance', [], 'finalized'))
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

if (!process.env.SKIP_RPL) {

const nodeIndicesToProcessRPL = nodeIndices.slice()
while (nodeIndicesToProcessRPL.length) {
  log(3, `${nodeIndicesToProcessRPL.length} nodes left to process RPL`)
  await Promise.all(
    nodeIndicesToProcessRPL.splice(0, MAX_CONCURRENT_NODES)
    .map(i => processNodeRPL(i))
  )
}
log(1, `totalEffectiveRplStake: ${totalEffectiveRplStake}`)

const nodeCollateralAmounts = new Map()
let totalCalculatedCollateralRewards = 0n
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

} // SKIP_RPL

if (currentIndex == 0) process.exit()

const ExecutionBlock = BigInt(await socketCall(['ExecutionBlock']))
const ConsensusBlock = BigInt(await socketCall(['ConsensusBlock']))

const bnStartEpoch = tryBigInt(process.env.OVERRIDE_START_EPOCH) || ConsensusBlock / slotsPerEpoch + 1n
log(2, `bnStartEpoch: ${bnStartEpoch}`)
/*
 * TODO: so far unused?
let bnStartBlock = bnStartEpoch * slotsPerEpoch
while (!(await checkSlotExists(bnStartBlock))) bnStartBlock++
log(2, `bnStartBlock: ${bnStartBlock}`)
const elStartBlock = await getBlockNumberFromSlot(bnStartBlock)
log(2, `elStartBlock: ${elStartBlock}`)
*/

const possiblyEligibleMinipoolIndexArray = new BigUint64Array(
  new SharedArrayBuffer(
    BigUint64Array.BYTES_PER_ELEMENT * (1 + (1 + 4 + 4) * parseInt(numberOfMinipools))
  )
)

import { Worker } from 'node:worker_threads'
const NUM_WORKERS = parseInt(process.env.NUM_WORKERS) || 12

const smoothingWorkers = Array.from(Array(NUM_WORKERS).keys()).map(i => {
  const data = {
    worker: new Worker('./smoothing.js', {workerData: possiblyEligibleMinipoolIndexArray}),
    promise: i,
    resolveWhenReady: null
  }
  data.worker.on('message', () => {
    if (typeof data.resolveWhenReady == 'function')
      data.resolveWhenReady(i)
  })
  return data
})

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
    log(3, `${left} nodes left to process smoothing`)
  const nodeAddress = nodeAddresses[i]
  const worker = await getWorker(smoothingWorkers)
  worker.postMessage({i, nodeAddress})
}

await Promise.all(smoothingWorkers.map(data => data.promise))
smoothingWorkers.forEach(data => data.worker.postMessage('exit'))

log(3, `${possiblyEligibleMinipoolIndexArray[0]} eligible minipools`)

const rocketPoolDuties = new Map()
const dutiesLock = makeLock()

const dutiesWorkers = Array.from(Array(NUM_WORKERS).keys()).map(w => {
  const data = {
    worker: new Worker('./duties.js', {workerData: possiblyEligibleMinipoolIndexArray}),
    promise: w,
    resolveWhenReady: null
  }
  data.worker.on('message', async (message) => {
    if (message === 'done') {
      if (typeof data.resolveWhenReady == 'function')
        data.resolveWhenReady(w)
      return
    }
    let i = 0
    while (i < message.length) {
      const slotIndex = message[i++]
      const committeeIndex = message[i++]
      const minipoolScore64s = []
      for (const _ of Array(4)) minipoolScore64s.push(message[i++])
      const minipoolScore = uint64sTo256(minipoolScore64s)
      const position = message[i++]
      const validatorIndex = message[i++]
      const minipoolAddress64s = []
      for (const _ of Array(4)) minipoolAddress64s.push(message[i++])
      const minipoolAddress = uint64sToAddress(minipoolAddress64s)
      const dutyKey = `${slotIndex},${committeeIndex}`
      await dutiesLock(() => {
        if (!rocketPoolDuties.has(dutyKey))
          rocketPoolDuties.set(dutyKey, [])
        log(3, `Storing duty ${dutyKey} ${validatorIndex}`)
        rocketPoolDuties.get(dutyKey).push(
          {minipoolAddress, validatorIndex, position, minipoolScore}
        )
      })
    }
  })
  return data
})

const intervalEpochsToGetDuties = Array.from(
  Array(parseInt(targetSlotEpoch - bnStartEpoch + 1n)).keys())
.map(i => bnStartEpoch + BigInt(i))

while (intervalEpochsToGetDuties.length) {
  if (intervalEpochsToGetDuties.length % 10 == 0)
    log(3, `${intervalEpochsToGetDuties.length} epochs left to get duties`)
  const worker = await getWorker(dutiesWorkers)
  worker.postMessage(intervalEpochsToGetDuties.shift())
}

await Promise.all(dutiesWorkers.map(data => data.promise))
dutiesWorkers.forEach(data => data.worker.postMessage('exit'))

log(3, `Got ${rocketPoolDuties.size} minipool duties`)
