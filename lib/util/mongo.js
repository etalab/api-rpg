const {MongoClient, ObjectID} = require('mongodb')

const MONGODB_URL = process.env.MONGODB_URL || 'mongodb://localhost'
const MONGODB_DBNAME = process.env.MONGODB_DBNAME || 'rpg'

class Mongo {
  async connect(options = {}) {
    this.client = await MongoClient.connect(MONGODB_URL, {
      socketTimeoutMS: (options.socketTimeout || 5 * 60) * 1000,
      reconnectTries: 1,
      useNewUrlParser: true
    })
    this.db = this.client.db(MONGODB_DBNAME)
  }

  disconnect(force) {
    return this.client.close(force)
  }
}

module.exports = new Mongo()
module.exports.ObjectID = ObjectID
