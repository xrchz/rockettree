import 'dotenv/config'
import { ethers } from 'ethers'
import PouchDB from 'pouchdb-node'

const verbosity = parseInt(process.env.VERBOSITY) || 2
const log = (v, s) => verbosity >= v ? console.log(s) : undefined

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
log(2, `Using Rocket Storage ${await rocketStorage.getAddress()}`)

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
log(1, `startTime: ${startTime}`)

const intervalTime = await cachedCall(rocketRewardsPool, 'getClaimIntervalTime', [], startBlock)
log(2, `intervalTime: ${intervalTime}`)

const latestBlockTime = await provider.getBlock('latest').then(b => BigInt(b.timestamp))
log(2, `latestBlockTime: ${latestBlockTime}`)

const timeSinceStart = latestBlockTime - startTime
const intervalsPassed = timeSinceStart / intervalTime
log(2, `intervalsPassed: ${intervalsPassed}`)

const genesisTime = genesisTimes.get(networkName)
const secondsPerSlot = 12n
const slotsPerEpoch = 32n

const endTime = startTime + (intervalTime * intervalsPassed)
log(1, `endTime: ${endTime}`)

const totalTimespan = endTime - genesisTime
log(2, `totalTimespan: ${totalTimespan}`)

let targetBcSlot = totalTimespan / secondsPerSlot
if (totalTimespan % secondsPerSlot) targetBcSlot++

const targetSlotEpoch = targetBcSlot / slotsPerEpoch
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
log(1, `targetElBlock: ${targetElBlock}`)
const targetElBlockTimestamp = await provider.getBlock(targetElBlock).then(b => BigInt(b.timestamp))
log(2, `targetElBlockTimestamp: ${targetElBlockTimestamp}`)

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
log(2, `pendingRewards: ${pendingRewards}`)
log(2, `collateralRewards: ${collateralRewards}`)
log(2, `oDaoRewards: ${oDaoRewards}`)
log(2, `pDaoRewards: ${pDaoRewards}`)

const rocketNodeManager = new ethers.Contract(
  await getRocketAddress('rocketNodeManager', targetElBlock),
  ['function getNodeCount() view returns (uint256)',
   'function getNodeAt(uint256) view returns (address)',
   'function getNodeRegistrationTime(address) view returns (uint256)'
  ],
  provider)

const rocketNodeStaking = new ethers.Contract(
  await getRocketAddress('rocketNodeStaking', targetElBlock),
  ['function getNodeRPLStake(address) view returns (uint256)'],
  provider)

const rocketMinipoolManager = new ethers.Contract(
  await getRocketAddress('rocketMinipoolManager', targetElBlock),
  ['function getNodeMinipoolAt(address, uint256) view returns (address)',
   'function getNodeMinipoolCount(address) view returns (uint256)',
   'function getMinipoolPubkey(address) view returns (bytes)',
   'function getMinipoolCount() view returns (uint256)'
  ],
  provider)

const getMinipool = (addr) =>
  new ethers.Contract(addr,
    ['function getStatus() view returns (uint8)',
     'function getUserDepositBalance() view returns (uint256)',
     'function getNodeDepositBalance() view returns (uint256)'
    ],
    provider)

const stakingStatus = 2

const rocketNetworkPrices = new ethers.Contract(
  await getRocketAddress('rocketNetworkPrices', targetElBlock),
  ['function getRPLPrice() view returns (uint256)'],
  provider)

const rocketDAOProtocolSettingsNode = new ethers.Contract(
  await getRocketAddress('rocketDAOProtocolSettingsNode', targetElBlock),
  ['function getMinimumPerMinipoolStake() view returns (uint256)',
   'function getMaximumPerMinipoolStake() view returns (uint256)'
  ],
  provider)

const nodeCount = await cachedCall(rocketNodeManager, 'getNodeCount', [], targetElBlock)
log(2, `nodeCount: ${nodeCount}`)
const nodeIndices = Array.from(Array(parseInt(nodeCount)).keys())
const nodeAddresses = await Promise.all(
  nodeIndices.map(i => cachedCall(rocketNodeManager, 'getNodeAt', [i], targetElBlock))
)
log(3, `nodeAddresses: ${nodeAddresses.slice(0, 5)}...`)

const ratio = await cachedCall(rocketNetworkPrices, 'getRPLPrice', [], targetElBlock)
const minCollateralFraction = await cachedCall(
  rocketDAOProtocolSettingsNode, 'getMinimumPerMinipoolStake', [], targetElBlock)
const maxCollateralFraction = await cachedCall(
  rocketDAOProtocolSettingsNode, 'getMaximumPerMinipoolStake', [], targetElBlock)

let totalEffectiveRplStake = 0n

const nodeEffectiveStakes = new Map()

const MAX_CONCURRENT_MINIPOOLS = parseInt(process.env.MAX_CONCURRENT_MINIPOOLS) || 10
const MAX_CONCURRENT_NODES = parseInt(process.env.MAX_CONCURRENT_NODES) || 10

async function processNode(i) {
  const nodeAddress = nodeAddresses[i]
  const minipoolCount = await cachedCall(
    rocketMinipoolManager, 'getNodeMinipoolCount', [nodeAddress], targetElBlock)
  log(3, `Processing ${nodeAddress}'s ${minipoolCount} minipools`)
  let eligibleBorrowedEth = 0n
  let eligibleBondedEth = 0n
  async function processMinipool(addr) {
    const minipool = getMinipool(addr)
    const minipoolStatus = await cachedCall(minipool, 'getStatus', [], targetElBlock)
    if (minipoolStatus != stakingStatus) return
    const pubkey = await cachedCall(
      rocketMinipoolManager, 'getMinipoolPubkey', [addr], targetElBlock)
    const validatorStatus = await getValidatorStatus(targetBcSlot, pubkey)
    const activationEpoch = validatorStatus.activation_epoch
    const exitEpoch = validatorStatus.exit_epoch
    const eligible = activationEpoch != 'FAR_FUTURE_EPOCH' && BigInt(activationEpoch) < targetSlotEpoch &&
                     (exitEpoch == 'FAR_FUTURE_EPOCH' || targetSlotEpoch < BigInt(exitEpoch))
    if (eligible) {
      const borrowedEth = await cachedCall(minipool, 'getUserDepositBalance', [], targetElBlock)
      eligibleBorrowedEth += BigInt(borrowedEth)
      const bondedEth = await cachedCall(minipool, 'getNodeDepositBalance', [], targetElBlock)
      eligibleBondedEth += BigInt(bondedEth)
    }
  }
  const minipoolIndicesToProcess = Array.from(Array(parseInt(minipoolCount)).keys())
  while (minipoolIndicesToProcess.length) {
    log(4, `${minipoolIndicesToProcess.length} minipools left for ${nodeAddress}`)
    await Promise.all(
      minipoolIndicesToProcess.splice(0, MAX_CONCURRENT_MINIPOOLS)
      .map(i => cachedCall(rocketMinipoolManager, 'getNodeMinipoolAt', [nodeAddress, i], targetElBlock)
                .then(addr => processMinipool(addr)))
    )
  }
  const minCollateral = eligibleBorrowedEth * minCollateralFraction / ratio
  const maxCollateral = eligibleBondedEth * maxCollateralFraction / ratio
  const nodeStake = BigInt(await cachedCall(
    rocketNodeStaking, 'getNodeRPLStake', [nodeAddress], targetElBlock)
  )
  let nodeEffectiveStake = nodeStake < minCollateral ? 0n :
                           nodeStake < maxCollateral ? nodeStake : maxCollateral
  const registrationTime = await cachedCall(
    rocketNodeManager, 'getNodeRegistrationTime', [nodeAddress], targetElBlock)
  const nodeAge = targetElBlockTimestamp - registrationTime
  if (nodeAge < intervalTime)
    nodeEffectiveStake = nodeEffectiveStake * nodeAge / intervalTime
  log(2, `${nodeAddress} effective stake: ${nodeEffectiveStake}`)
  nodeEffectiveStakes.set(nodeAddress, nodeEffectiveStake)
  totalEffectiveRplStake += nodeEffectiveStake
}

const nodeIndicesToProcess = nodeIndices.slice()
while (nodeIndicesToProcess.length) {
  log(3, `${nodeIndicesToProcess.length} nodes left to process`)
  await Promise.all(
    nodeIndicesToProcess.splice(0, MAX_CONCURRENT_NODES)
    .map(i => processNode(i))
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

const numberOfMinipools = cachedCall(rocketMinipoolManager, 'getMinipoolCount', [], targetElBlock)

log(1, `totalCalculatedCollateralRewards: ${totalCalculatedCollateralRewards}`)
if (collateralRewards - totalCalculatedCollateralRewards > numberOfMinipools)
  throw new Error('collateral calculation has excessive error')
}
