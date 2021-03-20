require('dotenv').config();

const VALID_FORMATS = ['Vinyl', 'CD', 'Cassette']

const fastify = require('fastify')()

fastify.register(require('fastify-mysql'), {
  promise: true,
  connectionString: `mysql://${process.env.DB_USER}:${process.env.DB_PASS}@${process.env.DB_HOST}/discogs`
})

/**
 * Search artists
 */

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

/**
 * Show artist
 */

 fastify.get('/artists/:id', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()

  const [rows, fields] = await connection.query(
    'SELECT id, name FROM artist WHERE id = ? limit 1', [req.params.id],
  )

  connection.release()

  if (!rows[0]) {
    return notFoundResponse(reply);
  }
  
  return rows[0]
})

/**
 * Get masters for artist
 */
fastify.get('/artists/:artist_id/masters', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()

  for (const required of ['format']) {
    if (!req.query[required]) {
      return validationResponse(reply, [
        {field: required, error: `Field ${required} is required`}
      ])
    }
  }
  
  if (VALID_FORMATS.indexOf(req.query.format) < 0) {
    return validationResponse(reply, [
      {field: 'format', error: `Value must be one of ${VALID_FORMATS.join(', ')}`}
    ])
  }

  const baseSQL = `select r.master_id as id, rf.name as format, ma.artist_id, a.name as artist_name, m.title, m.year from master_artist ma 
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

  let sql = `${baseSQL} ${params.map(e => e[0]).join(' AND ')} GROUP BY r.master_id order by m.year desc LIMIT 100;`

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

/**
 * Get releases for master
 */

fastify.get('/masters/:master_id/releases', async (req, reply) => {

  const connection = await fastify.mysql.getConnection()

  for (const required of ['format']) {
    if (!req.query[required]) {
      return validationResponse(reply, [
        {field: required, error: `Field ${required} is required`}
      ])
    }
  }
  
  if (VALID_FORMATS.indexOf(req.query.format) < 0) {
    return validationResponse(reply, [
      {field: 'format', error: `Value must be one of ${VALID_FORMATS.join(', ')}`}
    ])
  }

  let params = [
    [`ma.master_id = ?`, req.params.master_id]
  ];

  if (req.query.country) {
    params.push([`r.country = ?`, `${req.query.country}`]);
  }

  if (req.query.release_year) {
    let releaseYearParts = req.query.release_year.split(',');
    if (releaseYearParts[1]) {
      params.push([`r.release_year >= ?`, `${releasedParts[0]}`]);
      params.push([`r.release_year <= ?`, `${releasedParts[1]}`]);
    } else {
      params.push([`r.release_year = ?`, `${releasedParts[0]}`]);
    }
  }

  // format
  params.push(`r.id IN(select rf.release_id from release_format rf where rf.release_id = r.id and rf.name = ?)`, req.query.format)

  const sql = `select r.id as id, m.year, r.released, r.country, r.title as release_title, 
  r.master_id as master_id, 
  ma.artist_id, 
  a.name as artist_name, m.title from master_artist ma 
  inner join \`master\` m on ma.master_id = m.id
  inner join \`release\` r on r.master_id = m.id
  inner join artist a on a.id = ma.artist_id
  WHERE 
  ${params.map(e => e[0]).join(' AND ')}
  limit 100 order by r.released desc;`

  let [rows, fields] = await connection.query(
    sql, [req.params.master_id, req.query.format, req.query.country]
  )

  if (rows.length > 0) {
    rows = await eagerLoad(connection, Array.from(rows), 'release_identifier', 'release_id');
    rows = await eagerLoad(connection, Array.from(rows), 'release_label', 'release_id');
    rows = await eagerLoad(connection, Array.from(rows), 'release_format', 'release_id');
  }

  connection.release()
  return rows
})

/**
 * Show release by id
 */

fastify.get('/releases/:id', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()

  const sql = `select r.id as id, m.year, r.released, r.country, r.title as release_title, r.master_id as master_id, 
   ma.artist_id, a.name as artist_name, m.title from master_artist ma 
  inner join \`master\` m on ma.master_id = m.id
  inner join \`release\` r on r.master_id = m.id
  inner join artist a on a.id = ma.artist_id
  WHERE 
  r.id = ? limit 1;`

  let [rows, fields] = await connection.query(
    sql, [req.params.id]
  )

  if (!rows[0]) {
    connection.release()
    return notFoundResponse(reply)
  }

  rows = await eagerLoad(connection, Array.from(rows), 'release_identifier', 'release_id')
  rows = await eagerLoad(connection, Array.from(rows), 'release_label', 'release_id')
  rows = await eagerLoad(connection, Array.from(rows), 'release_format', 'release_id')
  
  connection.release()

  return rows[0]
})

async function eagerLoad(connection, parentRows, table, foreignKey) {
  let ids = parentRows.map(e => e.id);
  const [rows, fields] = await connection.query(`select * from \`${table}\` where ${foreignKey} IN(?);`, [ids])

  let collectionKey = `${table}s`

  parentRows.forEach(e => {
    e[collectionKey] = [];
  })

  let keyedByForeign = rows.reduce((a, e) => {
    
    if (!a[e[foreignKey]]) {
      a[e[foreignKey]] = [];
    }

    a[e[foreignKey]].push(e);
    
    return a;
  }, {});

  return parentRows.map(e => {
    e[collectionKey] = keyedByForeign[e.id]
    return e;
  })
}

/**
 * Release countries, for export purposes, very slow
 */

fastify.get('/countries', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()

  const sql = `select country, count(id) as country_count from \`release\` group by country order by country_count desc;`

  let [rows, fields] = await connection.query(
    sql, []
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

function validationResponse(reply, errors) {
  return reply
    .code(422)
    .header('Content-Type', 'application/json; charset=utf-8')
    .send({ error: `Missing required fields`, errors: errors})
}

function notFoundResponse(reply) {
  return reply
    .code(404)
    .header('Content-Type', 'application/json; charset=utf-8')
    .send({ error: `Not found`})
}