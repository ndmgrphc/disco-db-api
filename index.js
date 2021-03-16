require('dotenv').config();

const fastify = require('fastify')()

fastify.register(require('fastify-mysql'), {
  promise: true,
  connectionString: `mysql://${process.env.DB_USER}:${process.env.DB_PASS}@${process.env.DB_HOST}/discogs`
})

fastify.get('/artists', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()

  let query;
  if (req.query.search) {
    query = ['SELECT id, name FROM artist WHERE name LIKE ?', [`%${req.query.search}%`]];
  } else if (req.query.name) {
    query = ['SELECT id, name FROM artist WHERE name = ? limit 1', [`${req.query.name}`]];
  } else {
    query = ['SELECT id, name FROM artist WHERE name = ? limit 1', [`Taylor Swift`]];
  }

  const [rows, fields] = await connection.query(
    query[0], query[1],
  )
  connection.release()
  return rows[0]
})

fastify.get('/', async (req, reply) => {
  return 'Not an endpoint';
})

fastify.listen(process.env.PORT, err => {
  if (err) throw err
  console.log(`server listening on ${fastify.server.address().port}`)
})
