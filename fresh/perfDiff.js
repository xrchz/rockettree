import { readFileSync } from 'node:fs'
const official = JSON.parse(readFileSync('v10-29-perf.json')).minipoolPerformance
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
    const inOfficialOnlyEpochs = new Set(inOfficialOnly.map(x => BigInt(x) / 32n))
    const inRocketTreeOnly = missingAttestationSlots.filter(x => !officialMissing.includes(x))
    const inRocketTreeOnlyEpochs = new Set(inRocketTreeOnly.map(x => BigInt(x) / 32n))
    console.log(`${minipool} missing: ${inOfficialOnly} (${Array.from(inOfficialOnlyEpochs)}) vs ${inRocketTreeOnly} (${Array.from(inRocketTreeOnlyEpochs)})`)
    inOfficialOnlyEpochs.forEach(x => discrepancyEpochs.add(x))
    inRocketTreeOnlyEpochs.forEach(x => discrepancyEpochs.add(x))
  }
}
console.log(`discrepancy epochs: ${Array.from(discrepancyEpochs.values()).toSorted()}`)
