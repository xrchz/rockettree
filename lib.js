import { ethers } from 'ethers'

const genesisTimes = new Map()
genesisTimes.set('mainnet', 1606824023n)
genesisTimes.set('goerli', 1616508000n)

export const provider = new ethers.JsonRpcProvider(process.env.RPC_URL || 'http://localhost:8545')
export const networkName = await provider.getNetwork().then(n => n.name)
export const genesisTime = genesisTimes.get(networkName)

export const secondsPerSlot = 12n
export const slotsPerEpoch = 32n

export const bigIntToAddress = (n) => `0x${n.toString(16).padStart(40, '0')}`

export const iIdx = 0
export const iWait = 0
export const iWork = 1
export const iWorking = 2
export const iExit = 3
export const dIdx = 1
