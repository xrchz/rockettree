import { ethers } from 'ethers'

const genesisTimes = new Map()
genesisTimes.set('mainnet', 1606824023n)
genesisTimes.set('goerli', 1616508000n)

export const provider = new ethers.JsonRpcProvider(process.env.RPC_URL || 'http://localhost:8545')
export const networkName = await provider.getNetwork().then(n => n.name)
export const genesisTime = genesisTimes.get(networkName)

export const secondsPerSlot = 12n
export const slotsPerEpoch = 32n

const max64 = 2n ** 64n
export function uint256To64s(n) {
  const uint64s = []
  for (const _ of Array(4)) {
    uint64s.push(n % max64)
    n >>= 64n
  }
  return uint64s
}
export function uint64sTo256(a) {
  let n = 0n
  while (a.length) {
    n <<= 64n
    n += a.pop()
  }
  return n
}
export const addressToUint64s = s => uint256To64s(BigInt(s))
export const uint64sToAddress = a => `0x${uint64sTo256(a).toString(16).padStart(40, '0')}`

export const iIdx = 0
export const iWait = 0
export const iWork = 1
export const iWorking = 2
export const iExit = 3
export const dIdx = 1
