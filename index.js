require('dotenv').config();

const fastify = require('fastify')()

fastify.register(require('fastify-mysql'), {
  promise: true,
  connectionString: `mysql://${process.env.DB_USER}:${process.env.DB_PASS}@${process.env.DB_HOST}/discogs`
})

fastify.get('/artists', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()
  const [rows, fields] = await connection.query(
    'SELECT id, name FROM artist WHERE id=?', [req.query.id],
  )
  connection.release()
  return rows[0]
})

fastify.listen(process.env.PORT, err => {
  if (err) throw err
  console.log(`server listening on ${fastify.server.address().port}`)
})
