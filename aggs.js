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
async function run(aggs, timeFrom, timeTo, els_index, esClient, index_search, strDsl, objAdd, print) {
    try {
        let dsl = strDsl.replace("\"%{_from}\"", timeFrom.toString());
        dsl = dsl.replace("\"%{_to}\"", timeTo.toString());
        const query = JSON.parse(dsl);
        const search = {
            index: index_search,
            body: query
        };
        let flag = true;
        while (flag) {
            const {body} = await esClient.search(search);
            if ("aggregations" in body && isObject(body.aggregations) && aggs in body.aggregations) {
                let obj = body.aggregations[aggs];
                let array = "buckets" in obj ? obj.buckets : [];
                let after = "after_key" in obj ? obj.after_key : false;
                if (isObject(after)) {
                    search.body["aggs"][aggs].composite.after = after;
                } else {
                    flag = false;
                }
                let bulkBody = [];
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
                            } else if ("value" in item[key]) {
                                resp[key] = item[key].value != null ? item[key].value : undefined;
                            } else if ("buckets" in item[key]) {
                                if (item[key]["buckets"][0]["doc_count"])
                                    resp[key] = item[key]["buckets"][0]["doc_count"];
                            } else if ("values" in item[key]) {
                                resp[key] = item[key].values;
                            } else  if ("doc_count" in item[key]) {
                                resp[key] = item[key]["doc_count"];
                            } else {
                                resp[key] = item[key];
                            }
                        }
                        resp.time = timeFrom;
                        let data = objAdd && Object.keys(objAdd).length > 0 ? {...Object.expand(resp), ...objAdd} : Object.expand(resp);
                        if (print) {
                            console.log(JSON.stringify(data));
                        } else {
                            bulkBody.push({index: {_index: els_index}}, data);
                        }
                    }
                }
                if (bulkBody.length > 1) {
                    const {body: bulkResponse} = await esClient.bulk({index: els_index, body: bulkBody});
                    if (bulkResponse.errors) {
                        const erroredDocuments = []
                        bulkResponse.items.forEach((action, i) => {
                            const operation = Object.keys(action)[0]
                            if (action[operation].error) {
                                erroredDocuments.push({
                                    status: action[operation].status,
                                    error: action[operation].error,
                                    operation: body[i * 2],
                                    document: body[i * 2 + 1]
                                })
                            }
                        })
                        console.error(erroredDocuments);
                    }
                }
            }
        }
    } catch (e) {
        if ("name" in e && "meta" in e) {
            console.error(e);
            console.error(JSON.stringify(e));
        }
        else
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
            let timeFrom, timeTo;
            if ("lastDay" in config && config.lastDay) {
                let dateFrom = (d => new Date(d.setDate(d.getDate() - 1)))(new Date);
                timeFrom = new Date(dateFrom.getFullYear(), dateFrom.getMonth(), dateFrom.getDate()).getTime() / 1000;
                config.toDays = 1;
            } else {
                let arrDate = config.dateFrom.split("-");
                timeFrom = new Date(+arrDate[0], +arrDate[1] - 1, +arrDate[2]).getTime() / 1000;
            }
            if (fs.existsSync(config.query_file)) {
                const strDsl = fs.readFileSync(config.query_file, 'utf8');
                for (let i = 0; i < config.toDays; i++) {
                    timeTo = timeFrom + 24 * 60 * 60;
                    let fecha_index = new Date(timeFrom * 1000);
                    let index = fecha_index.getFullYear() + ".";
                    index += (fecha_index.getMonth() + 1) > 9 ? (fecha_index.getMonth() + 1) : '0' + (fecha_index.getMonth() + 1);
                    await run(config.aggs, timeFrom, timeTo, config.index_prefix + index, client, config.index_search, strDsl, config.attr, config.print);
                    timeFrom = timeTo;
                }
            } else {
                throw ("query file not exists: " +  config.query_file);
            }
        } else {
            throw ("config file not exists: " +  confFile);
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