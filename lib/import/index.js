#!/usr/bin/env node
require('dotenv').config()
const {join} = require('path')
const {bbox} = require('@turf/turf')
const mongo = require('../util/mongo')
const {createSpatialIndex} = require('../spatial-index')
const geojson2mongo = require('./geojson2mongo')

const dataPath = join(__dirname, '..', '..', 'data')
const parcellesPath = join(dataPath, 'parcelles.geojson')
const indexPath = join(dataPath, 'geo')

const collectionName = 'parcelles'

async function main() {
  const bboxes = []
  await mongo.connect()
  await mongo.db.createCollection(collectionName)
  const collection = mongo.db.collection(collectionName)
  await collection.dropIndexes()
  await collection.deleteMany({})
  await geojson2mongo(parcellesPath, collection, {
    transformFn: async obj => {
      const result = {
        _id: obj.ID_PARCEL,
        surface: obj.SURF_PARC,
        contour: obj._geometry
      }
      bboxes.push([result._id, bbox(result.contour)])
      // const simplifiedGeometry = await simplify(obj._geometry)
      // if (simplifiedGeometry && isValid(simplifiedGeometry)) {
      //   result.contour = simplifiedGeometry
      //   bboxes.push([result.id, bbox(simplifiedGeometry)])
      // }
      return result
    }
  })
  await mongo.disconnect()
  await createSpatialIndex(indexPath, bboxes)
  console.log('Terminé !')
}

main().catch(error => {
  console.error(error)
  process.exit(1)
})
