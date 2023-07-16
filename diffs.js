import { readFileSync } from 'node:fs'
const official = JSON.parse(readFileSync('/home/ramana/Downloads/rp-rewards-mainnet-11.json', {encoding: 'utf8'})).nodeRewards
const mine = JSON.parse(readFileSync('node-rewards.json', {encoding: 'utf8'}))
const mykeys = Object.keys(mine).map(a => a.toLowerCase())
const officialkeys = Object.keys(official)
for (const key of officialkeys) {
  if (!mykeys.includes(key))
    console.warn(`${key} in official but not mine`)
}
for (const key of mykeys) {
  if (!officialkeys.includes(key))
    console.warn(`${key} in mine but not official`)
}
for (const [node, {ETH, RPL}] of Object.entries(mine)) {
  const {smoothingPoolEth, collateralRpl, oracleDaoRpl} = official[node.toLowerCase()]
  const officialRPL = BigInt(collateralRpl) + BigInt(oracleDaoRpl)
  if (ETH != smoothingPoolEth)
    console.warn(`${node} ETH discrepancy: ${smoothingPoolEth} vs ${ETH} (diff ${BigInt(smoothingPoolEth) - BigInt(ETH)})`)
  if (RPL != officialRPL)
    console.warn(`${node} RPL discrepancy: ${officialRPL} vs ${RPL}`)
}
