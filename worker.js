import { ethers } from 'ethers'
import { bigIntToAddress, genesisTime, secondsPerSlot, iIdx, iWait, iWork, iWorking, iExit, dIdx } from './lib.js'

import { parentPort, workerData } from 'node:worker_threads'

const possiblyEligibleMinipools = new Map()
let i = 0
while (i < workerData.possiblyEligibleMinipoolIndices) {
  const [index, BInodeAddress, BIminipoolAddress] =
    workerData.possiblyEligibleMinipoolIndexArray.slice(i, 3 + i++)
  possiblyEligibleMinipools.set(index, {
    nodeAddress: bigIntToAddress(BInodeAddress),
    minipoolAddress: bigIntToAddress(BIminipoolAddress)
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
  Atomics.wait(workerData.signal, iIdx, iWait)
  if (Atomics.load(workerData.signal, iIdx) == iExit) process.exit()

  Atomics.store(workerData.signal, iIdx, iWorking)
  Atomics.notify(workerData.signal, iIdx)

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
    const [slotIndex, committeeIndex, numValidators] =
      workerData.committees.slice(i, i + 3)
    i += 3
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
      dutiesToReturn.set([slotIndex, committeeIndex, minipoolScore,
                          BigInt(position), validatorIndex, BigInt(minipoolAddress)],
                         eltsPerDuty * numDuties++)
    }
  }
  postDuties()
  Atomics.store(workerData.signal, iIdx, iWait)
  Atomics.notify(workerData.signal, iIdx)
}
