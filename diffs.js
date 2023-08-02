import { readFileSync } from 'node:fs'
const official = JSON.parse(readFileSync('/home/ramana/Downloads/rp-rewards-mainnet-12-partial.json', {encoding: 'utf8'})).nodeRewards
const mine = JSON.parse(readFileSync('node-rewards.json', {encoding: 'utf8'}))
const mykeys = Object.keys(mine).map(a => a.toLowerCase())
const officialkeys = Object.keys(official)
for (const key of officialkeys) {
  const {smoothingPoolEth, collateralRpl, oracleDaoRpl} = official[key]
  const offvals = [smoothingPoolEth, collateralRpl, oracleDaoRpl].join()
  if (!mykeys.includes(key) && offvals != '0,0,0')
    console.log(`${key} in official ${offvals} but not mine`)
}
for (const key of mykeys) {
  if (!officialkeys.includes(key))
    console.log(`${key} in mine but not official`)
}
for (const [node, {ETH, RPL}] of Object.entries(mine)) {
  const {smoothingPoolEth, collateralRpl, oracleDaoRpl} = official[node.toLowerCase()]
  const officialRPL = BigInt(collateralRpl) + BigInt(oracleDaoRpl)
  if (ETH != smoothingPoolEth)
    console.log(`${node} ETH discrepancy: ${smoothingPoolEth} vs ${ETH} (diff ${BigInt(smoothingPoolEth) - BigInt(ETH)})`)
  if (RPL != officialRPL)
    console.log(`${node} RPL discrepancy: ${officialRPL} vs ${RPL}`)
}
