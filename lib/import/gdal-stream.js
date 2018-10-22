const {Readable} = require('stream')
const gdal = require('gdal')

const wgs84 = gdal.SpatialReference.fromEPSG(4326)

class GDALReadableStream extends Readable {
  constructor(location) {
    super({objectMode: true})

    this.location = location
    this.ds = gdal.open(location)
    this.layer = this.ds.layers.get(0)
  }

  ensureWGS84(geometry) {
    geometry.transformTo(wgs84)
    return geometry.toObject()
  }

  // Implement ReadableStream interface
  _read() {
    setImmediate(() => {
      const feature = this.layer.features.next()
      if (feature) {
        this.push({
          type: 'Feature',
          properties: feature.fields.toObject(),
          geometry: this.ensureWGS84(feature.getGeometry())
        })
      } else {
        this.push(null)
      }
    })
  }
}

module.exports = {GDALReadableStream}
