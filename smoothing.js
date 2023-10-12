import { parentPort, workerData, threadId } from 'node:worker_threads'
import { log, stakingStatus, cachedCall, socketCall, addressToUint64s } from './lib.js'

const possiblyEligibleMinipoolIndexArray = workerData.value

const farPastTime = 0n
const farFutureTime = BigInt(1e18)

async function processNodeSmoothing(i, nodeAddress) {
  const minipoolCount = BigInt(await cachedCall(
    'rocketMinipoolManager', 'getNodeMinipoolCount', [nodeAddress], 'targetElBlock'))
  async function minipoolEligibility(i) {
    const minipoolAddress = await cachedCall(
      'rocketMinipoolManager', 'getNodeMinipoolAt', [nodeAddress, i], 'targetElBlock')
    const minipoolStatus = parseInt(await cachedCall(minipoolAddress, 'getStatus', [], 'targetElBlock'))
    const penaltyCount = parseInt(await cachedCall(
      'rocketNetworkPenalties', 'getPenaltyCount', [minipoolAddress], 'targetElBlock'))
    if (minipoolStatus == stakingStatus) {
      if (penaltyCount >= 3)
        return 'cheater'
      else {
        const pubkey = await cachedCall(
          'rocketMinipoolManager', 'getMinipoolPubkey', [minipoolAddress], 'finalized')
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
  const isOptedIn = await cachedCall(
    'rocketNodeManager', 'getSmoothingPoolRegistrationState', [nodeAddress], 'targetElBlock')
  const statusChangeTime = BigInt(await cachedCall(
    'rocketNodeManager', 'getSmoothingPoolRegistrationChanged', [nodeAddress], 'targetElBlock'))
  const optInTime = isOptedIn ? statusChangeTime : farPastTime
  const optOutTime = isOptedIn ? farFutureTime : statusChangeTime
  await socketCall(['nodeSmoothingTimes', nodeAddress, optInTime.toString(), optOutTime.toString()])
}

parentPort.on('message', async (msg) => {
  await processNodeSmoothing(msg.i, msg.nodeAddress)
  parentPort.postMessage('done')
})
