require('dotenv').config();

const VALID_FORMATS = ['Vinyl', 'CD', 'Cassette']

const fastify = require('fastify')()

fastify.register(require('fastify-mysql'), {
  promise: true,
  connectionString: `mysql://${process.env.DB_USER}:${process.env.DB_PASS}@${process.env.DB_HOST}:${process.env.DB_PORT}/discogs`
})

async function getGenresForReleaseId(connection, releaseId) {
  let genres = {};

  /**
   * Discogs mess.  Style and genre should be tied to master.  So we go up to the master and find
   * its releases and aggregate their styles and genres into one report:
   *
   * SELECT style from release_style WHERE release_id IN(select r.id
   * FROM `release` r where r.master_id IN(select master_id from `release` r where r.id = 226414))
   * GROUP BY style;
   */

  await Promise.all(['genre', 'style'].map(async source => {
    const [rows, fields] = await connection.query(
        `SELECT ${source} from release_${source} WHERE release_id IN(SELECT r.id 
     FROM \`release\` r WHERE r.master_id IN(SELECT master_id FROM \`release\` r WHERE r.id = ?)) 
     GROUP BY ${source};`, [releaseId]
    );

    if (rows.length === 0)
      return;

    rows.forEach(e => {
      genres[e[source]] = true;
    })
  }))

  return Object.keys(genres);
}

fastify.get('/release_genres/:id', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()

  const results = {
    tags: await getGenresForReleaseId(connection, req.params.id)
  };

  connection.release();

  return results;
})

/**
 * Search Catalog Number
 *
 * /catalog_numbers?format=Vinyl&search=XLLP785
 */

fastify.get('/catalog_numbers', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()

  if (req.query.search)
    req.query.search = req.query.search.replace(/[^a-zA-Z0-9]+/g,"");

  for (const required of ['format', 'search']) {
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

  let query = ['SELECT MIN(rl.catno) as catno, r.master_id as id, rf.name as format, \n' +
  'ma.artist_id, a.name as artist_name, m.title, m.year \n' +
  'FROM master_artist ma \n' +
  'inner join `master` m on ma.master_id = m.id \n' +
  'inner join `release` r on r.master_id = m.id \n' +
  'inner join release_format rf on r.id = rf.release_id \n' +
  'inner join release_label rl on r.id = rl.release_id \n' +
  'inner join artist a on a.id = ma.artist_id \n' +
  'WHERE rf.name = ? \n' +
  'AND rl.normalized_catno LIKE ? \n' +
  'GROUP BY r.master_id, ma.artist_id \n' +
  'ORDER BY r.master_id\n' +
  'LIMIT 25;', [req.query.format, `${req.query.search}%`]];

  const [rows, fields] = await connection.query(
      query[0], query[1],
  )
  connection.release()
  return rows
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

  let params = [
    [`ma.artist_id = ?`, req.params.artist_id]
  ];

  if (req.query.search) {
    params.push([`m.title like ?`, `%${req.query.search}%`]);
  }

  if (req.query.year) {
    let masterYearParts = req.query.year.split(',');
    if (masterYearParts[1]) {
      params.push([`m.year >= ?`, `${masterYearParts[0]}`]);
      params.push([`m.year <= ?`, `${masterYearParts[1]}`]);
    } else {
      params.push([`m.year = ?`, `${masterYearParts[0]}`]);
    }
  }

  // Pre-fetch master_id list

  let prefetchParams = params.concat([[`rf.name = ?`, req.query.format]]);

  if (req.query.country) {
    prefetchParams.push([`r.country = ?`, `${req.query.country}`])
  }

  let prefetchSql = `select r.master_id from \`release\` r 
  inner join release_format rf on rf.release_id = r.id 
  inner join master_artist ma on ma.master_id = r.master_id
  inner join master m on m.id = r.master_id
  where ${prefetchParams.map(e => e[0]).join(' AND ')} group by r.master_id;`;

  const [prefetchRows, prefetchFields] = await connection.query(
    prefetchSql, prefetchParams.map(e => e[1]),
  );

  if (req.query.debugPrefetch) {
    return [prefetchSql.replace("\n", ""), prefetchParams.map(e => e[1]), prefetchRows.map(e => e.master_id)];
  }
  
  if (prefetchRows.length === 0) {
    return {report: [], data: []}
  }

  let resultColumns = `m.id as id, ma.artist_id, a.name as artist_name, m.title, m.year`;

  const baseSQL = `select %COLUMNS% from master_artist ma 
  inner join \`master\` m on ma.master_id = m.id
  inner join artist a on a.id = ma.artist_id
  WHERE`

  // add master_id IN()
  params.push([`m.id IN(?)`, prefetchRows.map(e => e.master_id)])

  let reportSql = `${baseSQL.replace('%COLUMNS%', `m.year, count(m.id) as year_count`)} ${params.map(e => e[0]).join(' AND ')} GROUP BY m.year order by m.year;`

  //return [reportSql, params.map(e => e[1])];
  if (req.query.debugReport) {
    return [reportSql, params.map(e => e[1])];
  }

  const [reportRows, reportFields] = await connection.query(
    reportSql, params.map(e => e[1])
  );

  let sql = `${baseSQL.replace('%COLUMNS%', resultColumns)} ${params.map(e => e[0]).join(' AND ')} GROUP BY m.id order by m.year desc LIMIT 100;`

  const [rows, fields] = await connection.query(
    sql, params.map(e => e[1]),
  )

  connection.release()

  return {report: reportRows, data: rows}
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
    [`r.master_id = ?`, req.params.master_id]
  ];

  if (req.query.country) {
    params.push([`r.country = ?`, `${req.query.country}`]);
  }

  if (req.query.catno) {
    //params.push([`rl.normalized_catno = ?`, `${req.query.catno.replace('[^a-zA-Z0-9]', '')}`]);
    params.push([`r.id IN(select rl.release_id from release_label rl where rl.release_id = r.id and rl.normalized_catno = ?)`, req.query.catno.replace(/[^a-zA-Z0-9]/, '')])
  }

  let nullYear = false;
  if (req.query.year) {
    if (req.query.year === 'NULL') {
      nullYear = true;
    } else {
      let releaseYearParts = req.query.year.split(',');
      if (releaseYearParts[1]) {
        params.push([`r.release_year >= ?`, `${releaseYearParts[0]}`]);
        params.push([`r.release_year <= ?`, `${releaseYearParts[1]}`]);
      } else {
        params.push([`r.release_year = ?`, `${releaseYearParts[0]}`]);
      }
    }
  }

  // format
  params.push([`r.id IN(select rf.release_id from release_format rf where rf.release_id = r.id and rf.name = ?)`, req.query.format])

  let nullYearClause = '';
  if (nullYear) {
    nullYearClause = `AND r.release_year IS NULL`
  }

  // release_year report
  const reportSql = `select count(r.id) as year_count, r.release_year as year 
    FROM \`release\` r
    WHERE ${params.map(e => e[0]).join(' AND ')}
    ${nullYearClause}
    group by r.release_year
    order by year_count desc limit 200;`

  const [reportRows, reportFields] = await connection.query(
    reportSql, params.map(e => e[1])
  );

  const sql = `select r.id as id, r.released, r.country, r.title as release_title, r.release_year
  FROM \`release\` r
  WHERE 
  ${params.map(e => e[0]).join(' AND ')}
  ${nullYearClause}
  group by r.id
  order by r.release_year asc limit 100;`

  let [rows, fields] = await connection.query(
    sql, params.map(e => e[1])
  )

  /**
   * These are all hasMany because release CAN have multiple labels/sublabels and
   * multiple formats in box sets.
   */
  if (rows.length > 0) {
    rows = await eagerLoad(connection, Array.from(rows), 'release_identifier', 'release_id');
    rows = await eagerLoad(connection, Array.from(rows), 'release_label', 'release_id');
    rows = await eagerLoad(connection, Array.from(rows), 'release_format', 'release_id');
  }

  connection.release()

  return {report: reportRows, data: rows}
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

  rows[0].genres = await getGenresForReleaseId(connection, req.params.id);

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

fastify.get('/barcodes/:barcode', async(req, reply) => {
  const connection = await fastify.mysql.getConnection()

  const sql = `SELECT r.id, r.country, ri.description, r.title, a.name, rf.name, rf.descriptions
    FROM release_identifier ri inner join \`release\` r on r.id = ri.release_id 
    INNER JOIN master_artist ma on ma.master_id = r.master_id 
    INNER JOIN artist a on a.id = ma.artist_id 
    INNER JOIN release_format rf on r.id = rf.release_id
    WHERE type = 'Barcode' and normalized_value = ?;`;
  
  let [rows, fields] = await connection.query(
    sql, [req.params.barcode]
  );

  connection.release();

  return rows;
});

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