const {promisify} = require('util')
const writeFile = promisify(require('fs').writeFile)
const readFile = promisify(require('fs').readFile)
const Flatbush = require('flatbush')

function bufferToArrayBuffer(buffer) {
  return buffer.buffer.slice(0, buffer.byteLength)
}

async function createSpatialIndex(path, bboxes) {
  const index = new Flatbush(bboxes.length)
  for (const item of bboxes) {
    index.add(...item[1])
  }
  index.finish()
  await writeFile(path + '.flatbush', Buffer.from(index.data))
  await writeFile(path + '.mapping.json', JSON.stringify(bboxes.map(b => b[0])))
}

async function loadSpatialIndex(path) {
  const mapping = require(path + '.mapping.json')
  const buffer = await readFile(path + '.flatbush')
  const index = Flatbush.from(bufferToArrayBuffer(buffer))
  return {
    search(bbox) {
      return index.search(...bbox).map(i => mapping[i])
    }
  }
}

module.exports = {createSpatialIndex, loadSpatialIndex}
