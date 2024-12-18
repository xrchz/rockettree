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
for (const key of Object.keys(rockettree)) {
  if (rockettree[key].successfulAttestations == 0) continue
  if (!officialkeys.includes(key.toLowerCase()))
    console.log(`${key} in rockettree but not official`)
}
const discrepancyEpochs = new Set()
for (const [minipool, {attestationScore, missingAttestationSlots, successfulAttestations, bonus, consensusIncome}] of Object.entries(rockettree)) {
  const minipoolLower = minipool.toLowerCase()
  const {successfulAttestations: officialAttestations,
         attestationScore: officialScore,
         missingAttestationSlots: officialMissing,
         bonusEthEarned: officialBonus,
         consensusIncome: officialIncome,
        } = minipoolLower in official ? official[minipoolLower] :
    {successfulAttestations: 0, attestationScore: "0", missingAttestationSlots: [], bonusEthEarned: null, consensusIncome: null}
  if (BigInt(officialScore || 0) != BigInt(attestationScore || 0)) {
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
  if (BigInt(bonus || 0) != BigInt(officialBonus || 0)) {
    console.log(`${minipool} bonus discrepancy: ${bonus} vs ${officialBonus}`)
  }
  if (BigInt(consensusIncome || 0) != BigInt(officialIncome || 0)) {
    if (officialIncome)
      console.log(`${minipool} income discrepancy: ${consensusIncome} vs ${officialIncome}`)
  }
}
console.log(`discrepancy epochs: ${Array.from(discrepancyEpochs.values()).toSorted()}`)
