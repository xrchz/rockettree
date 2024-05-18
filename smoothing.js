import { parentPort, workerData, threadId } from 'node:worker_threads'
import { log, stakingStatus, socketCall, addressToUint64s } from './lib.js'

const possiblyEligibleMinipoolIndexArray = workerData.value

const farPastTime = 0n
const farFutureTime = BigInt(1e18)

async function processNodeSmoothing(i, nodeAddress) {
  const minipoolCount = BigInt(await socketCall(
    ['elState', 'rocketMinipoolManager', 'getNodeMinipoolCount', nodeAddress]))
  async function minipoolEligibility(i) {
    const minipoolAddress = await socketCall(
      ['elState', 'rocketMinipoolManager', 'getNodeMinipoolAt', `${nodeAddress},${i}`])
    const minipoolStatus = parseInt(await socketCall(['elState', minipoolAddress, 'getStatus']))
    const penaltyCount = parseInt(await socketCall(
      ['elState', 'rocketNetworkPenalties', 'getPenaltyCount', minipoolAddress]))
    if (minipoolStatus == stakingStatus) {
      if (penaltyCount >= 3)
        return 'cheater'
      else {
        const pubkey = await socketCall(
          ['elState', 'rocketMinipoolManager', 'getMinipoolPubkey', minipoolAddress])
        const index = BigInt(await socketCall(['beacon', 'getIndexFromPubkey', pubkey]))
        if (0 <= index) {
          const currentIndex = parseInt(Atomics.add(possiblyEligibleMinipoolIndexArray, 0, 1n))
          possiblyEligibleMinipoolIndexArray.set(
            [index, ...addressToUint64s(minipoolAddress)],
            1 + (1 + 3) * currentIndex)
        }
        return 'staking'
      }
    }
  }
  let staking = false
  for (const i of Array(parseInt(minipoolCount)).keys()) {
    const result = await minipoolEligibility(i)
    if (result === 'cheater') {
      log(3, `${nodeAddress} is a cheater`)
      return
    }
    else if (result === 'staking')
      staking = true
  }
  if (await socketCall(['nodeSmoothingTimes', nodeAddress, 'check']))
    return
  if (!staking) {
    log(4, `${nodeAddress} has no staking minipools: skipping`)
    return
  }
  const isOptedIn = await socketCall(
    ['elState', 'rocketNodeManager', 'getSmoothingPoolRegistrationState', nodeAddress])
  const statusChangeTime = BigInt(await socketCall(
    ['elState', 'rocketNodeManager', 'getSmoothingPoolRegistrationChanged', nodeAddress]))
  const optInTime = isOptedIn ? statusChangeTime : farPastTime
  const optOutTime = isOptedIn ? farFutureTime : statusChangeTime
  await socketCall(['nodeSmoothingTimes', nodeAddress, optInTime.toString(), optOutTime.toString()])
}

parentPort.on('message', async (msg) => {
  await processNodeSmoothing(msg.i, msg.nodeAddress)
  parentPort.postMessage('done')
})
