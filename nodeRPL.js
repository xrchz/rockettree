import { socketCall, cachedCall, multicall, stakingStatus, log } from './lib.js'
import { parentPort } from 'node:worker_threads'

const MAX_CONCURRENT_MINIPOOLS = parseInt(process.env.MAX_CONCURRENT_MINIPOOLS) || 128

const ratio = BigInt(await cachedCall('rocketNetworkPrices', 'getRPLPrice', [], 'targetElBlock'))
const minCollateralFraction = BigInt(await cachedCall(
  'rocketDAOProtocolSettingsNode', 'getMinimumPerMinipoolStake', [], 'targetElBlock'))
const maxCollateralFraction = BigInt(await cachedCall(
  'rocketDAOProtocolSettingsNode', 'getMaximumPerMinipoolStake', [], 'targetElBlock'))

const targetSlotEpoch = await socketCall(['targetSlotEpoch'])
const targetElBlockTimestamp = await socketCall(['targetElBlockTimestamp'])
const intervalTime = await socketCall(['intervalTime'])
const currentIndex = await socketCall(['currentIndex'])

function getEligibility(activationEpoch, exitEpoch) {
  const deposited = activationEpoch != 'FAR_FUTURE_EPOCH'
  const activated = deposited && BigInt(activationEpoch) < targetSlotEpoch
  const notExited = exitEpoch == 'FAR_FUTURE_EPOCH' || targetSlotEpoch < BigInt(exitEpoch)
  log(5, `deposited: ${deposited} activated: ${activated} notExited: ${notExited}`)
  return deposited && (currentIndex >= 15n || activated) && notExited
}

async function getNodeMinipoolInfo(nodeAddress) {
  const minipoolCount = BigInt(await cachedCall(
    'rocketMinipoolManager', 'getNodeMinipoolCount', [nodeAddress], 'targetElBlock'))
  log(4, `Processing ${nodeAddress}'s ${minipoolCount} minipools`)

  const minipoolIndicesToProcess = Array.from(Array(parseInt(minipoolCount)).keys())
  const nodeInfo = await multicall(
    minipoolIndicesToProcess.map(i => (
      {contractName: 'rocketMinipoolManager', fn: 'getNodeMinipoolAt', args: [nodeAddress, i]}
    )).concat([
      {contractName: 'rocketNodeStaking', fn: 'getNodeRPLStake', args: [nodeAddress]},
      {contractName: 'rocketNodeManager', fn: 'getNodeRegistrationTime', args: [nodeAddress]}
    ]),
    'targetElBlock'
  )
  const registrationTime = nodeInfo.pop()
  const nodeStake = nodeInfo.pop()

  const minipoolAddresses = []
  const minipoolInfo = []
  while (nodeInfo.length) {
    log(5, `${nodeInfo.length} minipools left for ${nodeAddress}...`)
    const minipoolAddressesBatch = nodeInfo.splice(0, MAX_CONCURRENT_MINIPOOLS)
    minipoolAddresses.push(...minipoolAddressesBatch)
    await multicall(
      minipoolAddressesBatch.flatMap(minipoolAddress =>
        [{contractName: minipoolAddress, fn: 'getStatus', args: []},
         {contractName: 'rocketMinipoolManager', fn: 'getMinipoolPubkey', args: [minipoolAddress]},
         {contractName: minipoolAddress, fn: 'getUserDepositBalance', args: []},
         {contractName: minipoolAddress, fn: 'getNodeDepositBalance', args: []}
        ]),
      'targetElBlock'
    ).then(results => minipoolInfo.push(...results))
  }

  let eligibleBorrowedEth = 0n
  let eligibleBondedEth = 0n
  for (const minipoolAddress of minipoolAddresses) {
    const [minipoolStatus, pubkey, borrowedEth, bondedEth] = minipoolInfo.splice(0, 4)
    if (minipoolStatus != stakingStatus) continue
    const validatorStatus = await socketCall(['beacon', 'getValidatorStatus', pubkey])
    if (getEligibility(validatorStatus.activation_epoch, validatorStatus.exit_epoch)) {
      eligibleBorrowedEth += borrowedEth
      eligibleBondedEth += bondedEth
    }
  }
  return {eligibleBorrowedEth, eligibleBondedEth, nodeStake, registrationTime}
}

async function processNodeRPL(nodeAddress) {
  const {eligibleBorrowedEth, eligibleBondedEth, nodeStake, registrationTime} =
    await getNodeMinipoolInfo(nodeAddress)

  const minCollateral = eligibleBorrowedEth * minCollateralFraction / ratio
  const maxCollateral = eligibleBondedEth * maxCollateralFraction / ratio
  let nodeEffectiveStake = nodeStake < minCollateral ? 0n :
                           nodeStake < maxCollateral ? nodeStake : maxCollateral
  const nodeAge = targetElBlockTimestamp - registrationTime
  if (nodeAge < intervalTime)
    nodeEffectiveStake = nodeEffectiveStake * nodeAge / intervalTime
  log(4, `${nodeAddress} effective stake: ${nodeEffectiveStake}`)
  parentPort.postMessage({nodeAddress, nodeEffectiveStake})
}

parentPort.on('message', async (msg) => {
  await processNodeRPL(msg)
  parentPort.postMessage('done')
})
