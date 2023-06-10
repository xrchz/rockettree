import 'dotenv/config'
import { ethers } from 'ethers'

const rocketStorageAddresses = new Map()
rocketStorageAddresses.set('mainnet', '0x1d8f8f00cfa6758d7bE78336684788Fb0ee0Fa46')
rocketStorageAddresses.set('goerli', '0xd8Cd47263414aFEca62d6e2a3917d6600abDceB3')

const genesisTimes = new Map()
genesisTimes.set('mainnet', 1606824023)
genesisTimes.set('goerli', 1616508000)

const beaconRpcUrl = process.env.BN_URL || 'http://localhost:5052'
const provider = new ethers.JsonRpcProvider(process.env.RPC_URL || 'http://localhost:8545')
const networkName = await provider.getNetwork().then(n => n.name)
const rocketStorage = new ethers.Contract(
  rocketStorageAddresses.get(networkName),
  ['function getAddress(bytes32) view returns (address)'], provider)
console.log(`Using Rocket Storage ${await rocketStorage.getAddress()}`)

const rocketRewardsPool = new ethers.Contract(
  await rocketStorage['getAddress(bytes32)'](ethers.id('contract.addressrocketRewardsPool')),
  ['function getClaimIntervalTimeStart() view returns (uint256)',
   'function getClaimIntervalTime() view returns (uint256)'
  ],
  provider)

const startBlock = parseInt(process.env.START_BLOCK) || 'latest'

const startTime = await rocketRewardsPool.getClaimIntervalTimeStart({blockTag: startBlock})
console.log(`startTime: ${startTime}`)

const intervalTime = await rocketRewardsPool.getClaimIntervalTime({blockTag: startBlock})
console.log(`intervalTime: ${intervalTime}`)

const latestBlockTime = await provider.getBlock('latest').then(b => b.timestamp)
console.log(`latestBlockTime: ${latestBlockTime}`)

const timeSinceStart = BigInt(latestBlockTime) - BigInt(startTime)
const intervalsPassed = timeSinceStart / intervalTime
console.log(`intervalsPassed: ${intervalsPassed}`)

const genesisTime = BigInt(genesisTimes.get(networkName))
const secondsPerSlot = 12n
const slotsPerEpoch = 32n

const fudgeFactor = 32n // TODO: figure out why this was wrong in interval 10
const endTime = startTime + (intervalTime * intervalsPassed) + fudgeFactor
console.log(`endTime: ${endTime}`)

const totalTimespan = endTime - genesisTime

let targetBcSlot = totalTimespan / secondsPerSlot
if (totalTimespan % secondsPerSlot) targetBcSlot++
async function checkSlotExists(slotNumber) {
  const url = new URL(`/eth/v1/beacon/headers/${slotNumber}`, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200 && response.status !== 404)
    console.warn(`Unexpected response status getting ${slotNumber} header: ${response.status}`)
  return response.status === 200
}
while (!(await checkSlotExists(targetBcSlot))) targetBcSlot--

const targetSlotEpoch = targetBcSlot / slotsPerEpoch
console.log(`targetBcSlot: ${targetBcSlot}`)
console.log(`targetSlotEpoch: ${targetSlotEpoch}`)

async function getBlockNumberFromSlot(slotNumber) {
  const url = new URL(`/eth/v1/beacon/blocks/${slotNumber}`, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status !== 200)
    console.warn(`Unexpected response status getting ${slotNumber} block: ${response.status}`)
  const json = await response.json()
  return BigInt(json.data.message.body.execution_payload.block_number)
}

const targetElBlock = await getBlockNumberFromSlot(targetBcSlot)
console.log(`targetElBlock: ${targetElBlock}`)
