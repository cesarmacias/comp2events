/*jslint node: true */
'use strict';

const {Client} = require('es7');
const fs = require("fs");
const argv = require('minimist')(process.argv.slice(2));

/*
    FUNCTION OBJECT TO EXPAND DOT ANNOTATION TO MULTI-LEVEL OBJECT
 */
(function () {
    function parseDotNotation(str, val, obj) {
        let currentObj = obj,
            keys = str.split("."),
            i, l = Math.max(1, keys.length - 1),
            key;

        for (i = 0; i < l; ++i) {
            key = keys[i];
            currentObj[key] = currentObj[key] || {};
            currentObj = currentObj[key];
        }

        currentObj[keys[i]] = val;
        delete obj[str];
    }
    Object.expand = function (obj) {
        for (const key in obj) {
            if (key.indexOf(".") !== -1) {
                parseDotNotation(key, obj[key], obj);
            }
        }
        return obj;
    };
})();

const isObject = function (a) {
    return (!!a) && (a.constructor === Object);
};
/*
    FUNCTION MAKE THE SEARCH QUERY AGGREGATION AND INSERT OR PRINT THE RESPONSE IN EVENT FORMAT
 */
async function run(config, timeFrom, timeTo, esClient, strDsl) {
    try {
        let dsl = strDsl.replace("\"%{_from}\"", timeFrom.toString());
        dsl = dsl.replace("\"%{_to}\"", timeTo.toString());
        const query = JSON.parse(dsl);
        const search = {
            index: config.index_search,
            body: query
        };
        let flag = true;
        while (flag) {
            const {body} = await esClient.search(search);
            if ("aggregations" in body && isObject(body.aggregations) && config.aggs in body.aggregations) {
                let obj = body.aggregations[config.aggs];
                let array = "buckets" in obj ? obj.buckets : [];
                let after = "after_key" in obj ? obj.after_key : false;
                if (isObject(after)) {
                    search.body["aggs"][config.aggs].composite.after = after;
                } else {
                    flag = false;
                }
                for await (const item of array) {
                    if (isObject(item)) {
                        let resp = {};
                        for (let key in item) {
                            if (key === "key" && isObject(item.key)) {
                                for (let field in item.key) {
                                    resp[field] = item.key[field];
                                }
                            } else if (key === "doc_count") {
                                resp.count = item[key];
                            } else if (key === config.exclude) {
                                continue;
                            } else if ("value" in item[key]) {
                                resp[key] = item[key].value != null ? item[key].value : undefined;
                            } else if ("buckets" in item[key]) {
                                if (item[key]["buckets"][0]["doc_count"])
                                    resp[key] = item[key]["buckets"][0]["doc_count"];
                            } else if ("values" in item[key]) {
                                resp[key] = item[key].values;
                            } else if ("doc_count" in item[key]) {
                                resp[key] = item[key]["doc_count"];
                            } else {
                                resp[key] = item[key];
                            }
                        }
                        resp.time = timeFrom;
                        let data = config.attr && Object.keys(config.attr).length > 0 ? {...Object.expand(resp), ...config.attr} : Object.expand(resp);
                        console.log(JSON.stringify(data));
                    }
                }
            }
        }
    } catch (e) {
        if ("name" in e && "meta" in e) {
            console.error(e);
            console.error(JSON.stringify(e));
        } else
            console.error(e);
    }
}
/*
    FUNCTION TO RED CONFIG FILE - PREPARE THE LOOP FOR SEARCH
 */
async function main(confFile) {
    try {
        if (fs.existsSync(confFile)) {
            const strConf = fs.readFileSync(confFile, 'utf8');
            const config = JSON.parse(strConf);
            const client = new Client({
                nodes: config.es_nodes,
                auth: {
                    username: config.username,
                    password: config.password
                },
                maxRetries: 5,
                requestTimeout: 90000
            });
            let delay = "delay" in config && config.delay > 0 ? config.delay * 60 : 0;
            let timeTo = Math.round(Date.now() / 1000 - delay);
            let timeFrom = Math.round(timeTo - config.interval * 60);
            if (fs.existsSync(config.query_file)) {
                const strDsl = fs.readFileSync(config.query_file, 'utf8');
                await run(config, timeFrom, timeTo, client, strDsl);
            } else {
                throw ("query file not exists: " + config.query_file);
            }
        } else {
            throw ("config file not exists: " + confFile);
        }
    } catch (e) {
        console.error(e);
    } finally {
        process.exit(0);
    }
}
/*
 STAR PROGRAM
 */
if ("config" in argv) {
    main(argv.config).catch(e => {
        console.error(e);
    });
} else {
    console.error("Not ARG --config with config file path");
}