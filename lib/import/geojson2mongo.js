const {promisify, callbackify} = require('util')
const {createReadStream} = require('fs')
const {createGunzip} = require('gunzip-stream')
const pump = promisify(require('pump'))
const through = require('through2')
const {parse} = require('../util/geojson')

function createTransform(transformFn) {
  if (!transformFn) {
    throw new Error('transformFn is required')
  }

  return through.obj(
    async (item, enc, cb) => {
      try {
        const result = await transformFn(item)
        cb(null, result)
      } catch (error) {
        cb(error)
      }
    }
  )
}

function flattenGeoJSON() {
  return through.obj(
    (feature, enc, cb) => {
      cb(null, {
        ...feature.properties,
        _geometry: feature.geometry
      })
    }
  )
}

function bucketize(bucketSize) {
  if (!bucketSize) {
    throw new Error('bucketSize is required')
  }

  let bucket = []
  return through.obj(
    function (item, enc, cb) {
      if (bucket.length === bucketSize) {
        this.push(bucket)
        bucket = []
      }
      bucket.push(item)
      cb()
    },
    function (cb) {
      if (bucket.length > 0) {
        this.push(bucket)
      }
      cb()
    }
  )
}

function insertBuckets(collection) {
  const iteratee = callbackify(async bucket => {
    await collection.insertMany(bucket)
  })
  return through.obj(iteratee)
}

function getInputStream(path) {
  return createReadStream(path)
}

async function geojson2mongo(geojsonPath, collection, options = {}) {
  const transformFn = options.transformFn || (async x => x)
  const bucketSize = options.bucketSize || 1000

  await pump(
    getInputStream(geojsonPath),
    createGunzip(),
    parse(),
    flattenGeoJSON(),
    createTransform(transformFn),
    bucketize(bucketSize),
    insertBuckets(collection)
  )
}

module.exports = geojson2mongo
