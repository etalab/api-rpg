/* eslint promise/prefer-await-to-then: off */

function wrap(handler) {
  return (req, res) => {
    handler(req)
      .then(result => res.send(result))
      .catch(error => {
        if (error.badRequest) {
          return res.status(400).send({
            code: 400,
            message: error.message
          })
        }
        if (error.response) {
          return res.status(error.statusCode).send(error.response.body)
        }
        if (error.status && error.body) {
          return res.status(error.status).send(error.body)
        }
        res.status(500).send({
          code: 500,
          message: error.message
        })
      })
  }
}

module.exports = wrap
