import { genesisTime, socketCall, secondsPerSlot, log } from './lib.js'
import { parentPort } from 'node:worker_threads'

async function processAttestation({minipoolAddress, slotIndex}) {
  const currentBond = BigInt(await socketCall(['elState', minipoolAddress, 'getNodeDepositBalance']))
  const currentFee = BigInt(await socketCall(['elState', minipoolAddress, 'getNodeFee']))
  const previousBond = BigInt(await socketCall(
    ['elState', 'rocketMinipoolBondReducer', 'getLastBondReductionPrevValue', minipoolAddress]))
  const previousFee = BigInt(await socketCall(
    ['elState', 'rocketMinipoolBondReducer', 'getLastBondReductionPrevNodeFee', minipoolAddress]))
  const lastReduceTime = BigInt(await socketCall(
    ['elState', 'rocketMinipoolBondReducer', 'getLastBondReductionTime', minipoolAddress]))
  const blockTime = genesisTime + secondsPerSlot * BigInt(slotIndex)
  const {bond, fee} = lastReduceTime > 0 && lastReduceTime > blockTime ?
    {bond: previousBond, fee: previousFee} :
    {bond: currentBond, fee: currentFee}
  const minipoolScore = (BigInt(1e18) - fee) * bond / BigInt(32e18) + fee
  parentPort.postMessage({minipoolAddress, slotIndex, minipoolScore})
}

parentPort.on('message', async (msg) => {
  await processAttestation(msg)
  parentPort.postMessage('done')
})
