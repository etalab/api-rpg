#!/usr/bin/env
require('dotenv').config()
const {join} = require('path')
const express = require('express')
const cors = require('cors')
const {bboxPolygon, area, bbox, booleanOverlap, featureCollection, feature} = require('@turf/turf')
const wrap = require('../util/wrap')
const mongo = require('../util/mongo')
const {loadSpatialIndex} = require('../spatial-index')
const {geometryIsValid} = require('./validation')
const {loadCultures} = require('./cultures')

const dataPath = join(__dirname, '..', '..', 'data')
const indexPath = join(dataPath, 'geo')

const app = express()
let spatialIndex
let cultures

function badRequest(message) {
  const err = new Error(message)
  err.badRequest = true
  return err
}

function asFeature(item) {
  const properties = {
    id: item._id,
    surface: item.surface,
    bio: item.bio,
    codeCulture: item.codeCulture
  }
  if (properties.codeCulture && properties.codeCulture in cultures) {
    properties.culture = cultures[properties.codeCulture]
  }
  return feature(item.contour, properties)
}

function asFeatureCollection(items) {
  return featureCollection(
    items.map(asFeature)
  )
}

app.use(cors())
app.use(express.json())

async function searchParcellesPolygon(polygon) {
  const boundingBox = bbox(polygon)
  const areaHa = area(polygon) / 10000
  if (areaHa > 1000) {
    throw badRequest('La requête doit porter sur une surface de moins de 1000 hectares.')
  }
  const candidates = spatialIndex.search(boundingBox)
  const parcelles = await mongo.db.collection('parcelles')
    .find({_id: {$in: candidates}})
    .toArray()

  return parcelles.filter(p => booleanOverlap(p.contour, polygon))
}

async function searchParcellesBBox(boundingBox) {
  const areaHa = area(bboxPolygon(boundingBox)) / 10000
  if (areaHa > 1000) {
    throw badRequest('La requête doit porter sur une surface de moins de 1000 hectares.')
  }
  const candidates = spatialIndex.search(boundingBox)
  const parcelles = await mongo.db.collection('parcelles')
    .find({_id: {$in: candidates}})
    .toArray()

  return parcelles
}

app.get('/parcelles', wrap(async req => {
  if (!req.query.bbox) {
    throw badRequest('bbox is required')
  }
  const bbox = req.query.bbox.split(',').map(Number.parseFloat)
  if (bbox.length !== 4 || bbox.some(Number.isNaN)) {
    throw badRequest('bbox is malformed')
  }
  const parcelles = await searchParcellesBBox(bbox)
  return asFeatureCollection(parcelles)
}))

app.post('/parcelles/search', wrap(async req => {
  if (!req.body.polygonIntersects) {
    throw badRequest('polygonIntersects is required')
  }
  // TODO: restrict to polygons
  if (!geometryIsValid(req.body.polygonIntersects)) {
    throw badRequest('Geometry is malformed')
  }
  const parcelles = await searchParcellesPolygon(req.body.polygonIntersects)
  return asFeatureCollection(parcelles)
}))

const port = process.env.PORT || 5000

async function main() {
  await mongo.connect()
  spatialIndex = await loadSpatialIndex(indexPath)
  cultures = await loadCultures()

  app.listen(port, () => {
    console.log('Start listening on port ' + port)
  })
}

main().catch(error => {
  console.error(error)
  process.exit(1)
})
