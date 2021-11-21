const nodeAxios = require('axios');
const HttpsProxyAgent = require("https-proxy-agent");
const cheerio = require('cheerio');
const tough = require('tough-cookie');

class Scogger {
    options;
    httpsAgent;

    constructor(options) {
        this.options = options;
        this.httpsAgent = new HttpsProxyAgent(this.options.p);
    }

    async ipCheck() {
        const axios = await this.getAxios();
        const res = await axios.get(this.descoober('aHR0cHM6Ly9hcGkuaXBpZnkub3JnP2Zvcm1hdD1qc29u'));
        return res.data;
    }

    descoober(scoobed) {
        // fck you want anyway
        return (Buffer.from(scoobed, 'base64')).toString();
    }

    async scogDat(rid) {
        let url = `${this.descoober('aHR0cHM6Ly93d3cuZGlzY29ncy5jb20vcmVsZWFzZS8=')}${rid}`;
        let headers = {
            Origin: url,
            Referer: url
        }

        let axios = await this.getAxios();
        const homeRes = await axios.get(url, {headers});
        const $ = cheerio.load(homeRes.data);
        const wow = $(`#release-stats ul li`);
        return wow.map((i, e) => {
            let result = {
                name: $(e).find('h4').first().text().replace(':', '').replace(' ', '_').toLowerCase(),
                value: null
            }

            let a = $(e).find('a').first();

            if (a.length > 0) {
                result.value = a.text();
            } else {
                let span = $(e).find('span').first();
                if (span) {
                    result.value = span.text();
                }
            }

            return result;
        }).toArray();
    }

    async getAxios() {
        const instance = nodeAxios.create({
            // WARNING: This value will be ignored.
            jar: new tough.CookieJar(),
            httpsAgent: this.httpsAgent,
            headers: {
                common: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                }
            }
        });
        instance.defaults.jar = new tough.CookieJar();
        return instance;
    }

}

module.exports = Scogger