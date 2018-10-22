#!/usr/bin/env node --max_old_space_size=4096
require('dotenv').config()
const {join} = require('path')
const {readdir} = require('fs-extra')
const bluebird = require('bluebird')
const {bbox} = require('@turf/turf')
const mongo = require('../util/mongo')
const {createSpatialIndex} = require('../spatial-index')
const geojson2mongo = require('./geojson2mongo')
const {GDALReadableStream} = require('./gdal-stream')

const dataPath = join(__dirname, '..', '..', 'data')
const indexPath = join(dataPath, 'geo')

const collectionName = 'parcelles'

async function listSourceFiles(srcPath) {
  const files = await readdir(srcPath)
  return files
    .filter(f => f.match(/^SURFACES-2018-PARCELLES-GRAPHIQUES-CONSTATEES_(\w{3})_.*\.zip$/))
    .map(f => join(srcPath, f))
}

async function importOneIn(srcPath, collection, bboxes) {
  console.log('Import de ' + srcPath)
  const gdalPath = '/vsizip/' + srcPath
  const geojsonStream = new GDALReadableStream(gdalPath)
  await geojson2mongo(geojsonStream, collection, {
    transformFn: async obj => {
      const result = {
        _id: `${obj.PACAGE}-${obj.NUM_ILOT}-${obj.NUM_PARCEL}`,
        pacage: obj.PACAGE,
        surface: obj.SURF_ADM,
        contour: obj._geometry,
        codeCulture: obj.CODE_CULTU,
        bio: obj.BIO === 1
      }
      bboxes.push([result._id, bbox(result.contour)])
      return result
    }
  })
}

async function main() {
  const bboxes = []
  console.log('Connection à la base de données')
  await mongo.connect()
  console.log('Connection à la base de données : OK')
  await mongo.db.createCollection(collectionName)
  const collection = mongo.db.collection(collectionName)
  console.log('La collection existe : OK')
  console.log('Suppression des indexes existants')
  await collection.dropIndexes()
  console.log('Suppression des indexes existants : OK')
  console.log('Suppression des données existantes')
  await collection.deleteMany({})
  console.log('Suppression des données existantes : OK')

  console.log('Import des nouvelles données')
  const sourcesFiles = await listSourceFiles(dataPath)
  await bluebird.map(sourcesFiles, f => importOneIn(f, collection, bboxes), {concurrency: 4})
  console.log('Import des nouvelles données : OK')

  await mongo.disconnect()
  console.log('Création de l’index spatial')
  await createSpatialIndex(indexPath, bboxes)
  console.log('Terminé !')
}

main().catch(error => {
  console.error(error)
  process.exit(1)
})
