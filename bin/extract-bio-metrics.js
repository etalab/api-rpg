#!/usr/bin/env node
require('dotenv').config()
const {join} = require('path')
const {readdir} = require('fs-extra')
const gdal = require('gdal')
const {uniq} = require('lodash')
const writeJsonFile = require('write-json-file')

async function listSourceFiles(srcPath) {
  const files = await readdir(srcPath)
  return files
    .filter(f => f.match(/^SURFACES-2018-PARCELLES-GRAPHIQUES-CONSTATEES_(\w{3})_.*\.zip$/))
    .map(f => join(srcPath, f))
}

function getCodeDepartement(srcPath) {
  const [, rawCodeDep] = srcPath.match(/SURFACES-2018-PARCELLES-GRAPHIQUES-CONSTATEES_(\w{3})_.*\.zip/)
  return rawCodeDep.charAt(0) === '0' ? rawCodeDep.substr(1) : rawCodeDep
}

function computeExploitantsBio(srcPath) {
  const dataset = gdal.open(`/vsizip/${srcPath}`)
  return uniq(
    dataset.layers.get(0).features
      .map(f => f.fields.toObject())
      .filter(p => p.BIO === 1 && p.PACAGE)
      .map(p => p.PACAGE)
  ).length
}

async function main() {
  const files = await listSourceFiles('./data')

  const metrics = files.map(f => {
    return {
      departement: getCodeDepartement(f),
      nombreExploitants: computeExploitantsBio(f)
    }
  })

  await writeJsonFile('bio-metrics.json', metrics)
}

main().catch(error => {
  console.error(error)
  process.exit(1)
})
