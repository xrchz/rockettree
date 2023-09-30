import { socketCall, cachedCall, stakingStatus, log } from './lib.js'
import { parentPort } from 'node:worker_threads'

const MAX_CONCURRENT_MINIPOOLS = parseInt(process.env.MAX_CONCURRENT_MINIPOOLS) || 10

const ratio = BigInt(await cachedCall('rocketNetworkPrices', 'getRPLPrice', [], 'targetElBlock'))
const minCollateralFraction = BigInt(await cachedCall(
  'rocketDAOProtocolSettingsNode', 'getMinimumPerMinipoolStake', [], 'targetElBlock'))
const maxCollateralFraction = BigInt(await cachedCall(
  'rocketDAOProtocolSettingsNode', 'getMaximumPerMinipoolStake', [], 'targetElBlock'))

const targetSlotEpoch = await socketCall(['targetSlotEpoch'])
const targetElBlockTimestamp = await socketCall(['targetElBlockTimestamp'])
const intervalTime = await socketCall(['intervalTime'])

async function processNodeRPL(nodeAddress) {
  const minipoolCount = BigInt(await cachedCall(
    'rocketMinipoolManager', 'getNodeMinipoolCount', [nodeAddress], 'targetElBlock'))
  log(4, `Processing ${nodeAddress}'s ${minipoolCount} minipools`)
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
    log(5, `${minipoolIndicesToProcess.length} minipools left for ${nodeAddress}`)
    // TODO: use multicall, at least to get the addresses, possibly also the pubkey. (status requires different blockTag)
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
  log(4, `${nodeAddress} effective stake: ${nodeEffectiveStake}`)
  parentPort.postMessage({nodeAddress, nodeEffectiveStake})
}

parentPort.on('message', async (msg) => {
  if (msg === 'exit') process.exit()
  await processNodeRPL(msg)
  parentPort.postMessage('done')
})
