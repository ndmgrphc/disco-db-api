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
    query = ['SELECT id, name FROM artist WHERE name LIKE ? limit 20', [`${req.query.search}%`]];
  } else if (req.query.name) {
    query = ['SELECT id, name FROM artist WHERE name = ? limit 1', [`${req.query.name}`]];
  } else {
    query = ['SELECT id, name FROM artist WHERE name = ? limit 1', [`Taylor Swift`]];
  }

  const [rows, fields] = await connection.query(
    query[0], query[1],
  )
  connection.release()
  return rows
})

fastify.get('/artists/:artist_id/masters', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()

  for (const required of ['country', 'format']) {
    if (!req.query[required]) {
      return reply
        .code(422)
        .header('Content-Type', 'application/json; charset=utf-8')
        .send({ error: `Missing required fields`, errors: [
          {field: required, error: `Field ${required} is required`}
        ]})
    }
  }

  const baseSQL = `select r.master_id as id, rf.name as format, ma.artist_id, a.name as artist_name, m.title from master_artist ma 
  inner join \`master\` m on ma.master_id = m.id
  inner join \`release\` r on r.master_id = m.id
  inner join release_format rf on r.id = rf.release_id
  inner join artist a on a.id = ma.artist_id
  WHERE`

  let params = [
    [`ma.artist_id = ?`, req.params.artist_id]
  ];

  if (req.query.search) {
    params.push([`m.title like ?`, `${req.query.search}%`]);
  }

  if (req.query.format) {
    params.push([`rf.name = ?`, `${req.query.format}`]);
  }

  if (req.query.country) {
    params.push([`r.country = ?`, `${req.query.country}`]);
  }

  let sql = `${baseSQL} ${params.map(e => e[0]).join(' AND ')} GROUP BY r.master_id LIMIT 50;`

  let query = [
    sql,
    params.map(e => e[1])
  ]

  const [rows, fields] = await connection.query(
    query[0], query[1],
  )
  connection.release()
  return rows
})

fastify.get('/', async (req, reply) => {
  return 'Not an endpoint';
})

fastify.listen(process.env.PORT, "0.0.0.0", err => {
  if (err) throw err
  console.log(`server listening on ${process.env.PORT}`)
})
