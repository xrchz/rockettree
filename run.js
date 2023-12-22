import 'dotenv/config'
import { ethers } from 'ethers'
import { EventEmitter } from 'node:events'
import { Worker, MessageChannel } from 'node:worker_threads'
import { provider, slotsPerEpoch, networkName, tryBigInt, makeLock, rpip30Interval,
         log, cacheWorker, cacheUserPort, socketCall, cachedCall } from './lib.js'
import { writeFileSync } from 'node:fs'

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

const NUM_WORKERS = parseInt(process.env.NUM_WORKERS) || 12
const workerPorts = new Map()

EventEmitter.captureRejections = true

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
    data.worker.once('error', (e) => {
      console.error(`Error in worker ${path} ${i}, exiting...`)
      console.error(e)
      process.exit(1)
    })
    data.worker.once('exit', () => {
      port1.close()
      port2.close()
    })
    return data
  })

cacheUserPort.on('message', msg => {
  if (typeof msg.id == 'object' && workerPorts.has(msg.id.threadId))
    workerPorts.get(msg.id.threadId).postMessage(msg)
})

async function getWorker(workers) {
  const i = await Promise.any(workers.map(data => data.promise))
  workers[i].promise = new Promise(resolve => {
    workers[i].resolveWhenReady = resolve
  })
  return workers[i].worker
}

const timestamp = () => Intl.DateTimeFormat('en-GB',
  {hour: 'numeric', minute: 'numeric', second: 'numeric'})
  .format(new Date())

const nodeEffectiveStakes = new Map()
const nodeWeights = new Map()
let totalEffectiveRplStake = 0n
let totalNodeWeight = 0n
const rpip30C = BigInt(Math.min(6, parseInt(currentIndex) - rpip30Interval + 1))

function processNodeRPL({nodeAddress, nodeEffectiveStake, nodeWeight}) {
  if (typeof nodeAddress != 'string' ||
      typeof nodeEffectiveStake != 'bigint' ||
      typeof nodeWeight != 'bigint')
    return
  nodeEffectiveStakes.set(nodeAddress, nodeEffectiveStake)
  totalEffectiveRplStake += nodeEffectiveStake
  if (rpip30Interval <= currentIndex) {
    nodeWeights.set(nodeAddress, nodeWeight)
    totalNodeWeight += nodeWeight
  }
}

const nodeRPLWorkers = process.env.SKIP_RPL ? [] : makeWorkers('./nodeRPL.js')
nodeRPLWorkers.forEach(data => data.worker.on('message', processNodeRPL))

const nodeIndicesToProcessRPL = process.env.SKIP_RPL ? [] : nodeIndices.slice()
while (nodeIndicesToProcessRPL.length) {
  if (nodeIndicesToProcessRPL.length % 10 == 0)
    log(3, `${timestamp()}: ${nodeIndicesToProcessRPL.length} nodes left to process RPL`)
  const i = nodeIndicesToProcessRPL.shift()
  const nodeAddress = nodeAddresses[i]
  const worker = await getWorker(nodeRPLWorkers)
  worker.postMessage(nodeAddress)
}
await Promise.all(nodeRPLWorkers.map(data => data.promise))
nodeRPLWorkers.forEach(data => data.worker.terminate())

log(1, `totalEffectiveRplStake: ${totalEffectiveRplStake}`)
log(1, `totalNodeWeight: ${totalNodeWeight}`)

const numberOfMinipools = BigInt(
  await cachedCall('rocketMinipoolManager', 'getMinipoolCount', [], 'targetElBlock'))

const nodeCollateralAmounts = new Map()
let totalCalculatedCollateralRewards = 0n

if (!process.env.SKIP_RPL && totalEffectiveRplStake && (currentIndex < rpip30Interval || totalNodeWeight)) {
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

const MAX_CONCURRENT_NODES = parseInt(process.env.MAX_CONCURRENT_NODES) || 10
const targetElBlockTimestamp = await socketCall(['targetElBlockTimestamp'])
const intervalTime = await socketCall(['intervalTime'])
const targetSlotEpoch = await socketCall(['targetSlotEpoch'])

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

const smoothingWorkers = makeWorkers('./smoothing.js', possiblyEligibleMinipoolIndexArray)

for (const i of nodeIndices) {
  const left = nodeIndices.length - i
  if (left % 10 == 0)
    log(3, `${timestamp()}: ${left} nodes left to process smoothing times`)
  const nodeAddress = nodeAddresses[i]
  const worker = await getWorker(smoothingWorkers)
  worker.postMessage({i, nodeAddress})
}

await Promise.all(smoothingWorkers.map(data => data.promise))
smoothingWorkers.forEach(data => data.worker.terminate())

log(3, `${possiblyEligibleMinipoolIndexArray[0]} eligible minipools`)

const dutiesWorkers = makeWorkers('./duties.js', possiblyEligibleMinipoolIndexArray)

const intervalEpochsToGetDuties = Array.from(
  Array(parseInt(targetSlotEpoch - bnStartEpoch + 1n)).keys())
.map(i => bnStartEpoch + BigInt(i))

while (intervalEpochsToGetDuties.length) {
  if (intervalEpochsToGetDuties.length % 10 == 0)
    log(3, `${timestamp()}: ${intervalEpochsToGetDuties.length} epochs left to get duties`)
  const epochIndex = intervalEpochsToGetDuties.shift().toString()
  if (await socketCall(['duties', epochIndex, 'check'])) continue
  const worker = await getWorker(dutiesWorkers)
  worker.postMessage(epochIndex)
}

await Promise.all(dutiesWorkers.map(data => data.promise))
dutiesWorkers.forEach(data => data.worker.terminate())

const minipoolAttestationsPerEpoch = new Map()
const minipoolAttestationsLock = makeLock()
let epochToCache = parseInt(bnStartEpoch)
const epochsChecked = new Set()
const epochsCached = new Set()

async function checkCache(epoch) {
  if (!epochsCached.has(epoch)) {
    if (await socketCall(['attestations', epoch.toString(), 'check']))
      epochsCached.add(epoch)
  }
  if (epochsCached.has(epoch)) {
    epochsChecked.add(epoch)
    epochsChecked.add(epoch + 1)
  }
}

async function updateEpochToCache() {
  while (epochsChecked.has(epochToCache) && epochsChecked.has(epochToCache + 1)) {
    if (!epochsCached.has(epochToCache)) {
      await socketCall(['attestations', epochToCache.toString(), minipoolAttestationsPerEpoch.get(epochToCache)])
      epochsCached.add(epochToCache)
    }
    minipoolAttestationsPerEpoch.delete(epochToCache)
    epochToCache++
  }
}

const attestationWorkers = makeWorkers('./attestations.js', {targetSlotEpoch, bnStartEpoch})
const processAttestation = (listener) =>
  async function ({minipoolAddress, slotIndex}) {
    if (slotIndex === 'done') {
      const checkedEpoch = parseInt(minipoolAddress)
      await minipoolAttestationsLock(async () => {
        epochsChecked.add(checkedEpoch)
        await updateEpochToCache()
      })
      return listener('done')
    }
    if (typeof minipoolAddress != 'string' || typeof slotIndex != 'number')
      return
    await minipoolAttestationsLock(() => {
      const epoch = parseInt(BigInt(slotIndex) / slotsPerEpoch)
      if (!minipoolAttestationsPerEpoch.has(epoch)) minipoolAttestationsPerEpoch.set(epoch, new Map())
      const minipoolAttestations = minipoolAttestationsPerEpoch.get(epoch)
      if (!minipoolAttestations.has(minipoolAddress)) minipoolAttestations.set(minipoolAddress, new Set())
      minipoolAttestations.get(minipoolAddress).add(slotIndex)
    })
  }

attestationWorkers.forEach(data => data.worker.on('message',
  processAttestation(data.worker.listeners('message')[0])))

const epochs = Array.from(
  Array(parseInt(targetSlotEpoch + 1n - bnStartEpoch + 1n)).keys())
.map(i => bnStartEpoch + BigInt(i))

while (epochs.length) {
  if (epochs.length % 10 == 0)
    log(3, `${timestamp()}: ${epochs.length} epochs left to process attestations`)
  const epoch = epochs.shift()
  const prevEpoch = epoch - 1n
  await checkCache(parseInt(epoch))
  await checkCache(parseInt(prevEpoch))
  if ((bnStartEpoch <= prevEpoch && !epochsCached.has(parseInt(prevEpoch))) ||
      (epoch <= targetSlotEpoch && !epochsCached.has(parseInt(epoch)))) {
    const worker = await getWorker(attestationWorkers)
    worker.postMessage(epoch.toString())
  }
}

await Promise.all(attestationWorkers.map(data => data.promise))
attestationWorkers.forEach(data => data.worker.terminate())

const minipoolScores = new Map()
const minipoolScoresByEpoch = new Map()
const dutiesToScoreByEpoch = new Map()
const minipoolScoresLock = makeLock()
minipoolScores.set('successfulAttestations', 0n)

const scoresWorkers = makeWorkers('./scores.js')

const addToMap = (map, key, num) =>
  map.set(key, map.has(key) ? map.get(key) + num : num)

function processScore ({minipoolAddress, slotIndex, minipoolScore}) {
  if (typeof minipoolAddress != 'string' ||
      typeof slotIndex != 'number' ||
      typeof minipoolScore != 'bigint') return
  const epoch = parseInt(BigInt(slotIndex) / slotsPerEpoch)
  const minipoolScoresForEpoch = minipoolScoresByEpoch.get(epoch)
  const dutiesToScore = dutiesToScoreByEpoch.get(epoch)
  return minipoolScoresLock(() => {
    addToMap(minipoolScoresForEpoch, minipoolAddress, minipoolScore)
    minipoolScoresForEpoch.set('successfulAttestations',
      minipoolScoresForEpoch.get('successfulAttestations') + 1n)
    dutiesToScore.delete(`${minipoolAddress},${slotIndex}`)
    if (!dutiesToScore.size) {
      const toSave = new Map()
      minipoolScoresForEpoch.forEach((minipoolScore, minipoolAddress) => {
        addToMap(minipoolScores, minipoolAddress, minipoolScore)
        toSave.set(minipoolAddress, minipoolScore.toString(16))
      })
      dutiesToScoreByEpoch.delete(epoch)
      minipoolScoresByEpoch.delete(epoch)
      return toSave
    }
  }).then(toSave => (
    toSave && socketCall(['scores', epoch.toString(), toSave])
  ))
}

scoresWorkers.forEach(data => data.worker.on('message', processScore))

const epochsToScoreAttestations = Array.from(
  Array(parseInt(targetSlotEpoch - bnStartEpoch + 1n)).keys())
.map(i => parseInt(bnStartEpoch) + i)

while (epochsToScoreAttestations.length) {
  if (epochsToScoreAttestations.length % 10 == 0)
    log(3, `${timestamp()}: ${epochsToScoreAttestations.length} epochs left to score attestations`)
  const epoch = epochsToScoreAttestations.shift()
  const epochStr = epoch.toString()
  if (await socketCall(['scores', epochStr, 'check'])) {
    const minipoolScoresForEpoch = await socketCall(['scores', epochStr])
    minipoolScoresForEpoch.forEach((minipoolScore, minipoolAddress) =>
      addToMap(minipoolScores, minipoolAddress, BigInt(`0x${minipoolScore}`)))
  }
  else {
    const minipoolScoresForEpoch = new Map()
    minipoolScoresByEpoch.set(epoch, minipoolScoresForEpoch)
    minipoolScoresForEpoch.set('successfulAttestations', 0n)
    const dutiesToScore = new Set()
    dutiesToScoreByEpoch.set(epoch, dutiesToScore)
    const minipoolAttestations = await socketCall(['attestations', epochStr])
    for (const [minipoolAddress, slots] of minipoolAttestations.entries()) {
      for (const slotIndex of slots.values()) {
        dutiesToScore.add(`${minipoolAddress},${slotIndex}`)
        const worker = await getWorker(scoresWorkers)
        worker.postMessage({minipoolAddress, slotIndex})
      }
    }
  }
}

await Promise.all(scoresWorkers.map(data => data.promise))
scoresWorkers.forEach(data => data.worker.terminate())

const successfulAttestations = minipoolScores.get('successfulAttestations')
minipoolScores.delete('successfulAttestations')
const totalMinipoolScore = Array.from(minipoolScores.values()).reduce((a, n) => a + n)

log(2, `successfulAttestations: ${successfulAttestations}`)
log(2, `totalMinipoolScore: ${totalMinipoolScore}`)

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

log(2, `totalNodeOpShare: ${totalNodeOpShare}`)

for (const [minipoolAddress, minipoolScore] of minipoolScores.entries()) {
  const minipoolEth = totalNodeOpShare * minipoolScore / totalMinipoolScore
  addNodeReward(await cachedCall(minipoolAddress, 'getNodeAddress', [], 'finalized'), 'ETH', minipoolEth)
  totalEthForMinipools += minipoolEth
}

log(2, `totalEthForMinipools: ${totalEthForMinipools}`)

const closeDb = socketCall(['close'])
cacheUserPort.close()
cacheWorker.unref()

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
rowHashes.push(`interval: ${currentIndex}`)
rowHashes.push(`start epoch: ${bnStartEpoch}`)
rowHashes.push(`target epoch: ${targetSlotEpoch}`)
writeFileSync('merkle-root.txt', rowHashes.join('\n'))
await closeDb
