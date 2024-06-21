import { readFileSync } from 'node:fs'
const official = JSON.parse(readFileSync('rp-minipool-performance-mainnet-22.json')).minipoolPerformance
const rockettree = JSON.parse(readFileSync('minipool-performance.json'))
const rockettreekeys = Object.keys(rockettree).map(a => a.toLowerCase())
const officialkeys = Object.keys(official)
for (const key of officialkeys) {
  if (official[key].successfulAttestations == 0) continue
  if (!rockettreekeys.includes(key))
    console.log(`${key} in official but not rockettree`)
}
for (const key of rockettreekeys) {
  if (!officialkeys.includes(key))
    console.log(`${key} in rockettree but not official`)
}
const discrepancyEpochs = new Set()
for (const [minipool, {attestationScore, missingAttestationSlots, successfulAttestations}] of Object.entries(rockettree)) {
  const minipoolLower = minipool.toLowerCase()
  const {successfulAttestations: officialAttestations,
         attestationScore: officialScore,
         missingAttestationSlots: officialMissing} = minipoolLower in official ? official[minipoolLower] :
    {successfulAttestations: 0, attestationScore: "0", missingAttestationSlots: []}
  if (BigInt(officialScore) != BigInt(attestationScore)) {
    console.log(`${minipool} discrepancy: ${BigInt(officialScore)} vs ${BigInt(attestationScore)}`)
    console.log(`${minipool} successes: ${BigInt(officialAttestations)} vs ${BigInt(successfulAttestations)}`)
    const inOfficialOnly = officialMissing.filter(x => !missingAttestationSlots.includes(x))
    const inRocketTreeOnly = missingAttestationSlots.filter(x => !officialMissing.includes(x))
    console.log(`${minipool} missing: ${inOfficialOnly} vs ${inRocketTreeOnly}`)
    for (const slot of inOfficialOnly)
      discrepancyEpochs.add(BigInt(slot) / 32n)
    for (const slot of inRocketTreeOnly)
      discrepancyEpochs.add(BigInt(slot) / 32n)
  }
}
console.log(`discrepancy epochs: ${Array.from(discrepancyEpochs.values()).toSorted()}`)
