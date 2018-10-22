const {join} = require('path')
const {createReadStream} = require('fs')
const csv = require('csv-parser')
const getStream = require('get-stream')

const CULTURES_PATH = join(__dirname, '..', '..', 'codes', 'CULTURE.csv')

async function loadCultures() {
  const culturesRows = await getStream.array(
    createReadStream(CULTURES_PATH)
      .pipe(csv({separator: ';'}))
  )
  return culturesRows.reduce((acc, row) => {
    acc[row.Code] = row.Libellé
    return acc
  }, {})
}

module.exports = {loadCultures}
