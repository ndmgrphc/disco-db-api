require('dotenv').config();

const ApiImport = require('./lib/ApiImport');
const Scogger = require('./lib/Scogger');

const scogger = new Scogger({
  p: {
    host: process.env.P_HOST,
    port: process.env.P_PORT,
    auth: process.env.P_AUTH
  }
});

const VALID_FORMATS = ['Vinyl', 'CD', 'Cassette', '8-Track Cartridge', 'Reel-to-Reel']

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

  const [releaseRows] = await connection.query(
      `select id, master_id from \`release\` where id = ?;`, [releaseId]
  );

  if (!releaseRows[0])
    return [];

  const masterId = releaseRows[0].master_id;

  /**
   * Discogs release genre and style are an insufferable shitshow. So taking
   * the first assigned genre with up to 4 styles seems to be at least usable
   * in some way.
   */
  let areas = [['genre', 1], ['style', 4]];

  await Promise.all(areas.map(async source => {
    let sql = `SELECT ${source[0]} from release_${source[0]} where release_id = ? LIMIT ${source[1]}`;

    if (masterId) {
      sql = `SELECT ${source[0]} from release_${source[0]} 
        WHERE release_id IN(
          SELECT r.id FROM \`release\` r 
              WHERE r.master_id IN(SELECT master_id FROM \`release\` r WHERE r.id = ?)
        ) 
        GROUP BY ${source[0]} LIMIT ${source[1]};`;
    }

    const [rows, fields] = await connection.query(
        sql, [releaseId]
    );

    if (rows.length === 0)
      return;

    rows.forEach(e => {
      genres[e[source[0]]] = true;
    })
  }))

  return Object.keys(genres);
}

fastify.get('/release_metas/:id', async(req, reply) => {
  try {
    return await scogger.scogDat(req.params.id);
  } catch (e) {
    reply.status(422).json({error: 'I cannot'});
  }
});

fastify.post('/api-imports/:id', async (req, reply) => {
  const connection = await fastify.mysql.getConnection();
  // https://api.discogs.com/releases/21017023
  const apiImport = new ApiImport(connection, false);

  return await apiImport.import(req.params.id);
});

fastify.get('/artist-hydrations', async (req, reply) => {
  // TODO: finish this
})

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
    req.query.search = req.query.search.replace(/[^a-zA-Z0-9]+/g,"").substr(0, 12);

  for (const required of ['format', 'search']) {
    if (!req.query[required]) {
      connection.release();
      return validationResponse(reply, [
        {field: required, error: `Field ${required} is required`}
      ])
    }
  }

  if (VALID_FORMATS.indexOf(req.query.format) < 0) {
    connection.release();
    return validationResponse(reply, [
      {field: 'format', error: `Value must be one of ${VALID_FORMATS.join(', ')}`}
    ])
  }

  const sql = `SELECT MIN(rl.catno) as catno, rl.label_name, r.master_id, COUNT(r.id) as release_count, MIN(r.id) as release_id, r.title as release_title, MIN(ra.artist_name) as artist_name, ra.artist_id
    FROM \`release\` r  
    INNER JOIN release_label rl on rl.release_id = r.id 
    INNER JOIN release_format rf on rf.release_id = r.id
    INNER JOIN release_artist ra ON r.id = ra.release_id
    WHERE (ra.role = '' OR ra.role is null) AND rf.name = ? AND rl.normalized_catno LIKE ?
    GROUP BY r.master_id, ra.artist_id, r.title, rl.normalized_catno, rl.label_name
    ORDER BY release_count DESC
    LIMIT 25;`;

  let query = [sql, [req.query.format, `${req.query.search}%`]];

  const [rows, fields] = await connection.query(
      query[0], query[1],
  )
  connection.release()
  return rows
})

/**
 * Search by artist
 *  GET /artists?search=Taylor+sw&format=Vinyl
 *  GET /artists?name=Taylor+Swift&format=Vinyl
 */

fastify.get('/artists', async (req, reply) => {
  const connection = await fastify.mysql.getConnection();

  let format = req.query.format;
  if (!format || ['Vinyl', 'CD', 'Cassette'].indexOf(format) < 0) {
    format = 'Vinyl';
  }

  let query;
  if (req.query.search) {
    // check variations
    const [varRows, varFields] = await connection.query(
        `select an.artist_id, a.name, COUNT(an.artist_id) as variation_count 
            from artist_namevariation an inner join artist a on an.artist_id = a.id 
            where an.name like ? group by an.artist_id order by variation_count 
            desc limit 10;`, `${req.query.search}%`,
    )

    let artistIds = [];
    if (varRows.length > 0)
      artistIds = varRows.map(e => e.artist_id);

    if (artistIds.length > 0) {
      query = [`select a.id, a.name, count(ra.id) as release_count 
                from artist a inner join release_artist ra on ra.artist_id = a.id 
                inner join release_format rf on ra.release_id = rf.release_id 
                where (a.name LIKE ? OR a.id IN(?)) and rf.name = ? 
                group by a.id order by release_count desc limit 10;`,
        [`${req.query.search}%`, artistIds, format]
      ];
    } else {
      query = [`select a.id, a.name, count(ra.id) as release_count 
                from artist a inner join release_artist ra on ra.artist_id = a.id 
                inner join release_format rf on ra.release_id = rf.release_id 
                where a.name LIKE ? and rf.name = ? 
                group by a.id order by release_count desc limit 10;`,
        [`${req.query.search}%`, format]
      ];
    }

  } else if (req.query.name) {
    query = ['SELECT id, name FROM artist WHERE name = ? limit 1', [`${req.query.name}`]];
  } else {
    query = ['SELECT id, name FROM artist WHERE name = ? limit 1', [`Taylor Swift`]];
  }

  const [rows, fields] = await connection.query(
    query[0], query[1],
  )

  connection.release();
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
 * Get tracks for release
 */

fastify.get('/tracks/:release_id', async (req, reply) => {
  if (!req.params.release_id) {
    return validationResponse(reply, [
      {field: 'release_id', error: `The release id is required`}
    ])
  }

  const connection = await fastify.mysql.getConnection()

  /**
   * track artist outer join
   *
   * select rt.id, rt.sequence, rt.position, rt.title, rt.duration, rta.artist_id, rta.artist_name, rta.role
   * from release_track rt left outer join release_track_artist rta on rt.id = rta.track_id_int
   * where (rta.role = '' OR rta.role IS NULL) and rt.release_id = 1939822;
   */
  const [trackRows, trackFields] = await connection.query(
      `select * from release_track where release_id = ? and position != '' and sequence is not null order by sequence;`, [req.params.release_id]
  );

  //select * from release_track_artist where track_id_int IN (23384742, 23384741, 23384740, 23384739)
  const [artistRows, artistFields] = await connection.query(
      `select * from release_track_artist where track_id_int IN (?);`, [trackRows.map(e => e.id)]
  );

  let artistRowsByTrackId = artistRows.reduce((a, e) => {
    /**
     * We need records without a role, these are actual artist records on compilations
     */
    if (!!e.role)
      return a;

    if (a[e.track_id])
      return a;

    a[e.track_id] = {
      artist_id: e.artist_id,
      artist_name: e.artist_name
    }

    connection.release();

    return a;
  }, {});

  trackRows.forEach(e => {
    e.artist = artistRowsByTrackId[e.id] ? artistRowsByTrackId[e.id] : null
  })

  reply.send({data: trackRows});
});

/**
 * Get masters for artist
 * @deprecated
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
    connection.release();
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
 * Get "albums" for artist
 * A hybrid of master and release search because some releases don't have masters.
 */

fastify.get('/artists/:artist_id/albums', async (req, reply) => {
  const connection = await fastify.mysql.getConnection()

  let format = req.query.format;
  if (!format || VALID_FORMATS.indexOf(format) < 0)
    format = 'Vinyl';

  let params = [
    [`ra.artist_id = ?`, req.params.artist_id],
    [`rf.name = ?`, format]
  ];

  if (req.query.search) {
    params.push([`r.title like ?`, `${req.query.search}%`]);
  }

  let sql = `SELECT min(r.id) as first_release_id, min(r.release_year) as first_release_year, r.title, m.id as master_id, m.year as master_year, count(m.id) as release_count
                FROM \`release\` r INNER JOIN release_artist ra ON r.id = ra.release_id
                INNER JOIN release_format rf ON rf.release_id = r.id
                LEFT JOIN \`master\` m ON r.master_id = m.id
                WHERE ${params.map(e => e[0]).join(' AND ')}
                GROUP BY r.title, m.id, m.year
                ORDER BY release_count DESC
                LIMIT 40;`;

  const [rows, fields] = await connection.query(
      sql, params.map(e => e[1]),
  )

  connection.release()

  return {data: rows}
});

fastify.get('/artists/:artist_id/format_report', async (req, reply) => {
  const connection = await fastify.mysql.getConnection();

  let format = req.query.format;
  if (!format || VALID_FORMATS.indexOf(format) < 0)
    format = 'Vinyl';

  let params = [
    [`ra.artist_id = ?`, req.params.artist_id],
    [`rf.name = ?`, format]
  ];

  if (req.query.title) {
    params.push([`r.title = ?`, `${req.query.title}`]);
  } else {
    return validationResponse(reply, [
      {field: 'title', error: `Field title (the exact album title) is required`}
    ])
  }

  if (req.query.catno) {
    let normalizedCatNo = req.query.catno.replace(/[^a-zA-Z0-9]+/g, '');
    params.push([`rl.normalized_catno = ?`, `${normalizedCatNo}`]);
  }

  if (req.query.text_string)
    params.push(['rf.text_string = ?', req.query.text_string]);

  if (req.query.label_name)
    params.push(['rl.label_name = ?', req.query.label_name]);

  // TODO: ignored for now
  if (req.query.master_year)
    params.push(['m.year = ?',req.query.master_year]);

  if (req.query.release_year) {
    let releaseYearParts = req.query.release_year.split(',');
    if (releaseYearParts[1]) {
      params.push([`r.release_year >= ?`, `${releaseYearParts[0]}`]);
      params.push([`r.release_year <= ?`, `${releaseYearParts[1]}`]);
    } else {
      params.push([`r.release_year = ?`, `${releaseYearParts[0]}`]);
    }
  }

  if (req.query.release_country) {
    let normalizedReleaseCountries = normalizeReleaseCountries(req.query.release_country);
    if (req.query.release_country)
      params.push(normalizedReleaseCountries);
  }

  //console.log('params', params);

  let sql = `SELECT r.title, r.release_year, r.release_country, rl.label_name, rl.normalized_catno, rf.text_string, count(r.id) as release_count
                FROM \`release\` r INNER JOIN release_artist ra ON r.id = ra.release_id
                INNER JOIN release_format rf ON rf.release_id = r.id
                INNER JOIN release_label rl ON rl.release_id = r.id
                WHERE ${params.map(e => e[0]).join(' AND ')}
                GROUP BY r.title, rl.label_name, rl.normalized_catno, rf.text_string, r.release_year, r.release_country
                ORDER BY release_count DESC
                LIMIT 40;`;

  //console.log('params', sql);

  const [rows, fields] = await connection.query(
      sql, params.map(e => e[1]),
  )

  connection.release()

  return {data: rows}
});

function normalizeReleaseCountries(input) {
  if (!Array.isArray(input)) {
    input = [input];
  }

  let validValues = input.filter(e => !!e);
  if (!validValues || validValues.length === 0)
    return null;

  if (input.indexOf(null) > -1 || input.indexOf('') > -1) {
    return [`(r.release_year is null OR r.release_year IN (?))`, validValues];
  } else {
    return [`r.release_year IN (?)`, validValues];
  }
}

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
    req.query.catno = req.query.catno.substr(0, 12);
    let normalizedCatNo = req.query.catno.replace(/[^a-zA-Z0-9]+/g, '');
    //params.push([`rl.normalized_catno = ?`, `${req.query.catno.replace('[^a-zA-Z0-9]', '')}`]);
    params.push([`r.id IN(select rl.release_id from release_label rl where rl.release_id = r.id and rl.normalized_catno LIKE ?)`, `${normalizedCatNo}%`])
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
    order by release_year desc limit 200;`

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
  const connection = await fastify.mysql.getConnection();

  const sql = `select r.id, r.released, r.country, r.title as release_title, r.master_id as master_id from \`release\` r where r.id = ? limit 1;`

  let [rows, fields] = await connection.query(
    sql, [req.params.id]
  )

  if (!rows[0]) {
    connection.release()
    return notFoundResponse(reply)
  }

  // add master details if exists
  if (rows[0].master_id) {
    const [masterRows, masterFields] = await connection.query(
          `select * from \`master\` where id = ?`, [rows[0].master_id]
    );
    if (masterRows[0]) {
      rows[0].year = masterRows[0].year;
      rows[0].title = masterRows[0].title;
    }
  }

  rows[0].artist = null;

  let [artistRows, artistFields] = await connection.query(
      `select artist_name as name, artist_id as id from release_artist where release_id = ? and (role = ? OR role is null);`, [rows[0].id, '']
  )

  if (artistRows[0])
    rows[0].artist = artistRows[0];

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