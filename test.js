require('dotenv').config();

const Scogger = require('./lib/Scogger');

const scogger = new Scogger({
    p: {
        host: process.env.P_HOST,
        port: process.env.P_PORT,
        auth: process.env.P_AUTH
    }
});

(async () => {
    let result = await scogger.scogDat(6075279);
    console.log(result);
})();