import { cachedCall, genesisTime, secondsPerSlot, log } from './lib.js'
import { parentPort } from 'node:worker_threads'

async function processAttestation({minipoolAddress, slotIndex}) {
  const currentBond = BigInt(await cachedCall(minipoolAddress, 'getNodeDepositBalance', [], 'targetElBlock'))
  const currentFee = BigInt(await cachedCall(minipoolAddress, 'getNodeFee', [], 'targetElBlock'))
  const previousBond = BigInt(await cachedCall(
    'rocketMinipoolBondReducer', 'getLastBondReductionPrevValue', [minipoolAddress], 'targetElBlock'))
  const previousFee = BigInt(await cachedCall(
    'rocketMinipoolBondReducer', 'getLastBondReductionPrevNodeFee', [minipoolAddress], 'targetElBlock'))
  const lastReduceTime = BigInt(await cachedCall(
    'rocketMinipoolBondReducer', 'getLastBondReductionTime', [minipoolAddress], 'targetElBlock'))
  const blockTime = genesisTime + secondsPerSlot * BigInt(slotIndex)
  const {bond, fee} = lastReduceTime > 0 && lastReduceTime > blockTime ?
    {bond: previousBond, fee: previousFee} :
    {bond: currentBond, fee: currentFee}
  const minipoolScore = (BigInt(1e18) - fee) * bond / BigInt(32e18) + fee
  parentPort.postMessage({minipoolAddress, minipoolScore})
}

parentPort.on('message', async (msg) => {
  if (msg === 'exit') process.exit()
  processAttestation(msg)
  parentPort.postMessage('done')
})
