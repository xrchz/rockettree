import { uint64sToAddress, socketCall, log } from './lib.js'
import { parentPort, workerData, threadId } from 'node:worker_threads'

const possiblyEligibleMinipoolIndexArray = workerData.value
const possiblyEligibleMinipools = new Map()
let i = 0
const possiblyEligibleMinipoolIndices = possiblyEligibleMinipoolIndexArray[0]
while (i < possiblyEligibleMinipoolIndices) {
  const [index, minipoolAddress0, minipoolAddress1, minipoolAddress2] =
    possiblyEligibleMinipoolIndexArray.slice(1 + (1 + 3) * i, 1 + (1 + 3) * ++i)
  possiblyEligibleMinipools.set(parseInt(index),
    uint64sToAddress([minipoolAddress0, minipoolAddress1, minipoolAddress2]))
}

async function processCommittees(epochIndex) {
  const duties = []
  const committees = await socketCall(['beacon', 'getCommittees', epochIndex])
  log(3, `${threadId} processing ${committees.length} committees for ${epochIndex}`)
  for (const committee of committees) {
    const slotIndex = BigInt(committee.slot)
    const committeeIndex = BigInt(committee.index)
    duties.push(slotIndex.toString(), committeeIndex.toString())
    const lengthIndex = duties.length
    duties.push(0)
    for (const [position, validatorIndexStr] of committee.validators.entries()) {
      const validatorIndex = parseInt(validatorIndexStr)
      if (!possiblyEligibleMinipools.has(validatorIndex)) continue
      const minipoolAddress = possiblyEligibleMinipools.get(validatorIndex)
      duties[lengthIndex]++
      duties.push(minipoolAddress, position.toString())
    }
    if (duties[lengthIndex])
      duties[lengthIndex] = duties[lengthIndex].toString()
    else
      duties.splice(-3, 3)
  }
  await socketCall(['duties', epochIndex, duties.join()])
}

parentPort.on('message', async (msg) => {
  if (msg === 'exit') process.exit()
  await processCommittees(msg)
  parentPort.postMessage('done')
})
