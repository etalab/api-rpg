const geojsonhint = require('@mapbox/geojsonhint')
const {feature} = require('@turf/turf')

function geometryIsValid(geometry) {
  const f = feature(geometry, {})
  const hints = geojsonhint.hint(f, {precisionWarning: false})
  return hints.length === 0
}

module.exports = {geometryIsValid}
