import { socketCall, stakingStatus, rpip30Interval, oneEther, ln, log } from './lib.js'
import { ethers } from 'ethers'
import { parentPort } from 'node:worker_threads'

const ratio = BigInt(await socketCall(['elState', 'rocketNetworkPrices', 'getRPLPrice']))
const minCollateralFraction = BigInt(await socketCall(['elState', 'rocketDAOProtocolSettingsNode', 'getMinimumPerMinipoolStake']))
const maxCollateralFraction = 150n * 10n ** 16n

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
  const minipoolCount = BigInt(await socketCall(
    ['elState', 'rocketMinipoolManager', 'getNodeMinipoolCount', nodeAddress]))
  log(4, `Processing ${nodeAddress}'s ${minipoolCount} minipools`)

  const minipoolIndicesToProcess = Array.from(Array(parseInt(minipoolCount)).keys())
  const minipoolAddresses = await Promise.all(minipoolIndicesToProcess.map(i =>
    socketCall(['elState', 'rocketMinipoolManager', 'getNodeMinipoolAt', `${nodeAddress},${i}`])
  ))
  const nodeStake = BigInt(await socketCall(['elState', 'rocketNodeStaking', 'getNodeRPLStake', nodeAddress]))
  const registrationTime = BigInt(await socketCall(['elState', 'rocketNodeManager', 'getNodeRegistrationTime', nodeAddress]))

  let eligibleBorrowedEth = 0n
  let eligibleBondedEth = 0n
  for (const minipoolAddress of minipoolAddresses) {
    const minipoolStatus = await socketCall(['elState', minipoolAddress, 'getStatus'])
    if (minipoolStatus != stakingStatus) continue
    // console.time(`getValidatorStatus ${pubkey}`)
    const pubkey = await socketCall(['elState', 'rocketMinipoolManager', 'getMinipoolPubkey', minipoolAddress])
    const validatorStatus = await socketCall(['beacon', 'getValidatorStatus', pubkey])
    // console.timeEnd(`getValidatorStatus ${pubkey}`)
    if (getEligibility(validatorStatus.activation_epoch, validatorStatus.exit_epoch)) {
      const borrowedEth = BigInt(await socketCall(['elState', minipoolAddress, 'getUserDepositBalance']))
      const bondedEth = BigInt(await socketCall(['elState', minipoolAddress, 'getNodeDepositBalance']))
      eligibleBorrowedEth += borrowedEth
      eligibleBondedEth += bondedEth
    }
  }
  return {eligibleBorrowedEth, eligibleBondedEth, nodeStake, registrationTime}
}

const oneHundredEther = 100n * oneEther
const thirteenEther = 13n * oneEther
const fifteenEther = 15n * oneEther
const thirteen6137Ether = ethers.parseEther('13.6137')

function getNodeWeight(eligibleBorrowedEth, nodeStake) {
  if (currentIndex < rpip30Interval) return 0n
  if (!eligibleBorrowedEth) return 0n
  const stakedRplValueInEth = nodeStake * ratio / oneEther
  const percentOfBorrowedEth = stakedRplValueInEth * oneHundredEther / eligibleBorrowedEth
  if (percentOfBorrowedEth <= fifteenEther)
    return 100n * stakedRplValueInEth
  else
    return ((thirteen6137Ether + 2n * ln(percentOfBorrowedEth - thirteenEther)) * eligibleBorrowedEth) / oneEther
}

async function processNodeRPL(nodeAddress) {
  const {eligibleBorrowedEth, eligibleBondedEth, nodeStake, registrationTime} =
    await getNodeMinipoolInfo(nodeAddress)

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
  log(4, `${nodeAddress} effective stake: ${nodeEffectiveStake}, weight: ${nodeWeight}`)
  parentPort.postMessage({nodeAddress, nodeEffectiveStake, nodeWeight})
}

parentPort.on('message', async (msg) => {
  await processNodeRPL(msg)
  parentPort.postMessage('done')
})
