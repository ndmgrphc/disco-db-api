const Axios = require('axios');
const nodePath = require('path');
const nodeFS = require('fs');

class ApiImport {
    conn;
    dryRun;

    constructor(conn, dryRun) {
        this.conn = conn;
        this.dryRun = dryRun;
    }

    async import(id) {
        let logEntries = [];

        //masters: https://api.discogs.com/masters/2383843
        this.id = id;
        const data = await this.fetch(id);

        // ensure artist
        await this.ensure('artist', {
            id: data.artists[0].id,
            name: data.artists[0].name,
        });

        // ensure master
        if (data.master_id) {
            await this.ensure('master', {
                id: data.master_id,
                title: data.title,
                year: data.year,
                main_release: data.id,
                data_quality: 'Burntable'
            });
        }

        // ensure release
        // | id    | title           | released | country | notes | data_quality | main | master_id | status   | release_year |
        await this.ensure('release', {
            id: data.id,
            title: data.title,
            released: data.released,
            country: data.country,
            notes: '',
            data_quality: 'Burntable',
            master_id: data.master_id,
            status: 'Accepted',
            release_year: data.year
        });

        // release_artist
        /**
         * `id` bigint unsigned NOT NULL AUTO_INCREMENT,
         `release_id` int NOT NULL,
         `artist_id` int NOT NULL,
         `artist_name` text,
         `extra` int NOT NULL,
         `anv` text,
         `position` int DEFAULT NULL,
         `join_string` text,
         `role` text,
         `tracks` text,
         */
        if (data.artists && data.artists.length > 0) {
            await this.ensure('release_artist', data.artists.map(e => {
                return {
                    release_id: data.id,
                    artist_id: e.id,
                    artist_name: e.name,
                    extra: 1
                }
            }), ['release_id', 'artist_id']);

            // master artist
            /**
             * `id` bigint unsigned NOT NULL AUTO_INCREMENT,
             `master_id` int NOT NULL,
             `artist_id` int NOT NULL,
             `artist_name` text,
             `anv` text,
             `position` int DEFAULT NULL,
             `join_string` text,
             `role` text,
             */
            if (data.master_id) {
                await this.ensure('master_artist', data.artists.map(e => {
                    return {
                        master_id: data.master_id,
                        artist_id: e.id,
                        artist_name: e.name,
                    }
                }), ['master_id', 'artist_id']);
            }
        }

        // label
        let catNo = null;
        if (data.labels && data.labels[0]) {
            catNo = data.labels[0].catno;
            await this.ensure('label', {
                id: data.labels[0].id,
                name: data.labels[0].name,
            });
        }

        // release_label
        if (catNo) {
            await this.ensure('release_label', {
                release_id: data.id,
                label_id: data.labels[0].id,
                label_name: data.labels[0].name,
                catno: catNo,
                normalized_catno: catNo.toUpperCase().replace(/[^0-9A-Z]/g, '').substring(0, 12)
            }, ['release_id', 'label_id']);
        }

        // release_format
        /**
         * `id` bigint unsigned NOT NULL AUTO_INCREMENT,
         `release_id` int NOT NULL,
         `name` text,
         `qty` decimal(10,0) DEFAULT NULL,
         `text_string` text,
         `descriptions` text,
         */
        if (data.formats && data.formats.length > 0) {
            await this.ensure('release_format', data.formats.map(e => {
                return {
                    release_id: data.id,
                    name: e.name,
                    qty: e.qty,
                    descriptions: e.descriptions ? e.descriptions.join(' ') : 'LP'
                }
            }), ['release_id', 'name']);
        }

        // release_identifier
        if (data.identifiers && data.identifiers.length > 0) {
            await this.ensure('release_identifier', data.identifiers.map(e => {
                return {
                    release_id: data.id,
                    type: e.type,
                    value: e.value,
                    description: e.description
                }
            }), ['release_id', 'type']);
        }

        if (data.genres && data.genres.length > 0) {
            await this.ensure('release_genre', data.genres.map(e => {
                return {
                    release_id: data.id,
                    genre: e
                }
            }), ['release_id', 'genre']);
        }

        // release_track
        /**
         * `id` bigint unsigned NOT NULL AUTO_INCREMENT,
         `release_id` int NOT NULL,
         `sequence` int NOT NULL,
         `position` text,
         `parent` int DEFAULT NULL,
         `title` text,
         `duration` text,
         `track_id` text,
         */
        if (data.tracklist && data.tracklist.length > 0) {
            await this.ensure('release_track', data.tracklist.filter(track => {
                return !!track.position && track.type_ === 'track';
            }).sort((a, b) => a.position > b.position && 1 || -1).map((e, idx) => {
                return {
                    release_id: data.id,
                    sequence: idx + 1,
                    title: e.title,
                    position: e.position,
                    duration: e.duration
                }
            }), ['release_id', 'title']);
        }

        return data;
    }

    async ensure(table, records, keys, stringTyped) {
        if (!Array.isArray(records))
            records = [records];

        if (!keys)
            keys = ['id'];

        let inClauses = keys.map(key => {
            return [`${key} IN(?)`, records.map(record => record[key])]
        });

        const [existingRecords, fields] = await this.conn.query(
            `select ${keys.join(',')} from \`${table}\` where ${inClauses.map(clause => clause[0]).join(' AND ')};`, inClauses.map(clause => clause[1]),
        )

        let recordsToInsert;
        if (existingRecords.length !== 0) {
            recordsToInsert = records.filter(record => {
               let searchString = keys.reduce((a, k) => {
                   a = a + record[k];
                   return a;
               }, '');

               console.log('searchString', searchString);

                return !Array.from(existingRecords).find(existing => {
                    let searchStringExisting = keys.reduce((a,k) => {
                        a = a + existing[k];
                        return a;
                    }, '');

                    return searchStringExisting === searchString;
                })
            });
        } else {
            recordsToInsert = [...records];
        }

        if (!recordsToInsert || recordsToInsert.length === 0) {
            console.debug(`No records to insert for ${table}`, records);
            return [];
        }

        let insertKeys = Object.keys(records[0]);

        let sql = `INSERT INTO \`${table}\` (${insertKeys.join(',')}) VALUES ?`;
        let insertableRecords = recordsToInsert.map(record => {
            return insertKeys.reduce((a, k) => {
                a.push(record[k]);
                return a;
            }, [])
        });

        if (this.dryRun) {
            console.debug(`DRY RUN:`, this.conn.format(sql, [insertableRecords]));
            return;
        }

        try {

            await this.conn.query(sql, [insertableRecords]);
            return true;
        } catch (e) {
            console.error(`SQL Failed: ${sql}`, e.message);
            return false;
        }
    }

    async fetch(id) {
        const cachedRelease = await this.getCachedRelease(id);
        if (cachedRelease)
            return cachedRelease;

        const axios = Axios.create({
            headers: {
                'User-Agent': 'BURNTABLE: Community data supplement capped at 5 reqs / min, if issues please contact apeterson@burntable.com or discogs user @brntbl'
            }
        });

        const response = await axios.get(`https://api.discogs.com/releases/${id}`);
        if (response.status !== 200) {
            console.error(`Discogs api for https://api.discogs.com/releases/${id} returned ${response.status} with headers ${JSON.stringify(response.headers)}`);
            return null;
        }

        await this.cacheRelease(id, response.data);

        return response.data;
    }

    async cacheRelease(id, data) {
        const cacheFile = nodePath.resolve(__dirname, `../data/${id}.json`);
        nodeFS.writeFileSync(cacheFile, JSON.stringify(data));
        return cacheFile;
    }

    async getCachedRelease(id) {
        const cacheFile = nodePath.resolve(__dirname, `../data/${id}.json`);
        if (!nodeFS.existsSync(cacheFile))
            return false;

        try {
            return JSON.parse(nodeFS.readFileSync(cacheFile, 'utf-8'));
        } catch (e) {
            console.error(`ApiImport failed to decode json in file at ${cacheFile} ${e.message}`);
            return null;
        }
    }
}

module.exports = ApiImport;