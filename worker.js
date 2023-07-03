import { ethers } from 'ethers'
import { uint64sToAddress, addressToUint64s, uint256To64s, genesisTime, secondsPerSlot,
         iIdx, iIdle, iReady, iWorking, iExit, dIdx } from './lib.js'
import { parentPort, workerData, threadId } from 'node:worker_threads'

const possiblyEligibleMinipools = new Map()
let i = 0
while (i < workerData.possiblyEligibleMinipoolIndices) {
  const [index, nodeAddress0, nodeAddress1, nodeAddress2, nodeAddress3,
         minipoolAddress0, minipoolAddress1, minipoolAddress2, minipoolAddress3] =
    workerData.possiblyEligibleMinipoolIndexArray.slice(i, 1 + 4 + 4 + i++)
  possiblyEligibleMinipools.set(index, {
    nodeAddress: uint64sToAddress([nodeAddress0, nodeAddress1, nodeAddress2, nodeAddress3]),
    minipoolAddress: uint64sToAddress([minipoolAddress0, minipoolAddress1, minipoolAddress2, minipoolAddress3])
  })
}

const minipoolInfo = new Map()
async function getMinipoolInfo(minipoolAddress, key) {
  if (!minipoolInfo.has(minipoolAddress))
    minipoolInfo.set(minipoolAddress, new Map())
  const info = minipoolInfo.get(minipoolAddress)
  if (info.has(key)) return info.get(key)
  parentPort.postMessage({minipoolAddress, key})
  const value = await new Promise((resolve) => parentPort.on('message', resolve))
  info.set(key, value)
  return value
}

const nodeSmoothingTimes = new Map()
async function getNodeSmoothingTimes(nodeAddress) {
  if (nodeSmoothingTimes.has(nodeAddress))
    return nodeSmoothingTimes.get(nodeAddress)
  parentPort.postMessage(nodeAddress)
  const value = await new Promise((resolve) => parentPort.on('message', resolve))
  nodeSmoothingTimes.set(nodeAddress, value)
  return value
}

while (true) {
  let sig = Atomics.compareExchange(workerData.signal, iIdx, iReady, iWorking)
  if (sig == iExit) process.exit()
  if (sig != iReady) {
    Atomics.wait(workerData.signal, iIdx, sig)
    continue
  }

  const numCommittees = Atomics.load(workerData.signal, dIdx)
  const eltsPerDuty = 6
  const maxByteLength = eltsPerDuty * BigUint64Array.BYTES_PER_ELEMENT * numCommittees
  let dutiesToReturn = new BigUint64Array(new ArrayBuffer(0, {maxByteLength}))
  function postDuties() {
    parentPort.postMessage(dutiesToReturn, [dutiesToReturn.buffer])
    dutiesToReturn = new BigUint64Array(new ArrayBuffer(0, {maxByteLength}))
  }
  let committeesLeft = numCommittees
  let numDuties = 0
  i = 0
  while (committeesLeft--) {
    console.log(`${threadId} ${committeesLeft} committeesLeft`)
    const [slotIndex, committeeIndex, BInumValidators] =
      workerData.committees.slice(i, i + 3)
    const numValidators = parseInt(BInumValidators)
    i += 3
    if (!(typeof genesisTime === typeof secondsPerSlot &&
          typeof secondsPerSlot === typeof slotIndex)) {
      console.error(`Failure with ${genesisTime} ${secondsPerSlot} ${slotIndex}`)
      process.exit(1)
    }
    const blockTime = genesisTime + secondsPerSlot * slotIndex
    const validators = workerData.committees.slice(i, i + numValidators)
    i += numValidators
    for (const [position, validatorIndex] of validators.entries()) {
      if (!possiblyEligibleMinipools.has(validatorIndex)) continue
      const {nodeAddress, minipoolAddress} = possiblyEligibleMinipools.get(validatorIndex)
      const {optInTime, optOutTime} = await getNodeSmoothingTimes(nodeAddress)
      if (blockTime < optInTime || blockTime > optOutTime) continue
      const statusTime = await getMinipoolInfo(minipoolAddress, 'getStatusTime')
      if (blockTime < statusTime) continue
      const currentBond = await getMinipoolInfo(minipoolAddress, 'getNodeDepositBalance')
      const currentFee = await getMinipoolInfo(minipoolAddress, 'getNodeFee')
      const previousBond = await getMinipoolInfo(minipoolAddress, 'getLastBondReductionPrevValue')
      const previousFee = await getMinipoolInfo(minipoolAddress, 'getLastBondReductionPrevNodeFee')
      const lastReduceTime = await getMinipoolInfo(minipoolAddress, 'getLastBondReductionTime')
      const {bond, fee} = lastReduceTime > 0 && lastReduceTime > blockTime ?
                          {bond: previousBond, fee: previousFee} :
                          {bond: currentBond, fee: currentFee}
      const minipoolScore = (BigInt(1e18) - fee) * bond / BigInt(32e18) + fee
      let newByteLength = dutiesToReturn.byteLength + eltsPerDuty * BigUint64Array.BYTES_PER_ELEMENT
      if (newByteLength >= maxByteLength) {
        newByteLength -= dutiesToReturn.byteLength
        postDuties()
        numDuties = 0
      }
      dutiesToReturn.buffer.resize(newByteLength)
      dutiesToReturn.set([slotIndex, committeeIndex,
                          ...uint256To64s(minipoolScore),
                          BigInt(position), validatorIndex,
                          ...addressToUint64s(minipoolAddress)],
                         eltsPerDuty * numDuties++)
    }
  }
  postDuties()
  sig = Atomics.compareExchange(workerData.signal, iIdx, iWorking, iIdle)
  if (sig != iWorking) {
    console.error(`Got wrong signal ${sig} before idle in ${threadId}`)
    process.exit(1)
  }
  Atomics.notify(workerData.signal, iIdx)
}
