/*
Description: 
JRM - This is a generic JSON Relational Mapping library that wrap crud sql statements via database api.
conn - database connection
table - the table that is populated
data - json data including columns and values
where condition - json data that defines where columns and values
*/

import logger from "../logger.js";
import { config } from '../load-config.js';

class Db {
    constructor(conn) {
        this.conn = conn;
    }

    static checkCurrentKeyword(string) {
        let foundCurrent = false;
        const upperString = string.toUpperCase();

        if (upperString.includes('`CURRENT DATE`') && !upperString.includes('\\`CURRENT DATE\\`')) {
            foundCurrent = true;
        } else if (upperString.includes('`CURRENT_DATE`') && !upperString.includes('\\`CURRENT_DATE\\`')) {
            foundCurrent = true;
        } else if (upperString.includes('`CURRENT TIMESTAMP`') && !upperString.includes('\\`CURRENT TIMESTAMP\\`')) {
            foundCurrent = true;
        } else if (upperString.includes('`CURRENT_TIMESTAMP`') && !upperString.includes('\\`CURRENT_TIMESTAMP\\`')) {
            foundCurrent = true;
        }

        return foundCurrent;
    }

    static caseInsensitiveReplace(str, search, replacement) {
        const regex = new RegExp(search, 'gi'); // 'g' for global, 'i' for insensitive
        return str.replace(regex, replacement);
    }

    static unescapeCurrentKeyword(string) {

        string = Db.caseInsensitiveReplace(string, '`CURRENT DATE`', 'CURRENT DATE');
        string = Db.caseInsensitiveReplace(string, '`CURRENT_DATE`', 'CURRENT_DATE');
        string = Db.caseInsensitiveReplace(string, '`CURRENT TIMESTAMP`', 'CURRENT TIMESTAMP');
        string = Db.caseInsensitiveReplace(string, '`CURRENT_TIMESTAMP`', 'CURRENT_TIMESTAMP');

        return string;
    }

    static conditionsToString(where, databaseType = '') {
        // where is an array of json object
        // Example: where = [{"createtime": {"operator": ">", "value": "CURRENT DATE - 10 DAYS", "placeholder": "N"}}, {"createtime":  {"operator": ">", "value": "CURRENT DATE", "placeholder": "N"}}]

        let strWhere = "";
        let arrValues = [];
        let paramIndex = 0;

        where.forEach(condition => {
            if (condition) {
                for (const [k, c] of Object.entries(condition)) {
                    if (typeof c['value'] === 'string') {
                        // escape single quote
                        c['value'] = c['value'].replace("''", "'");
                        if (Db.checkCurrentKeyword(c['value'])) {
                            // CURRENT keyword cannot be put in placeholder
                            strWhere += ` AND ${k} ${c['operator']} ${Db.unescapeCurrentKeyword(c['value'])}`;
                        } else {
                            if (c['placeholder'] === 'N') {
                                strWhere += ` AND ${k} ${c['operator']} '${c['value']}'`;
                            } else {
                                // Y is default for placeholder
                                if (['postgres', 'postgresql'].includes(databaseType)) {
                                    // postgresql uses $1, $2, etc. for placeholder
                                    paramIndex++;
                                    strWhere += ` AND ${k} ${c['operator']} $${paramIndex}`;
                                    arrValues.push(c['value']);
                                } else {
                                    paramIndex++;
                                    strWhere += ` AND ${k} ${c['operator']} ?`;
                                    arrValues.push(c['value']);
                                }
                            }
                        }
                    } else {
                        if (c['placeholder'] === 'N') {
                            strWhere += ` AND ${k} ${c['operator']} ${c['value']}`;
                        } else {
                            // Y is default for placeholder
                            if (['postgres', 'postgresql'].includes(databaseType)) {
                                // postgresql uses $1, $2, etc. for placeholder
                                paramIndex++;
                                strWhere += ` AND ${k} ${c['operator']} $${paramIndex}`;
                                arrValues.push(c['value']);
                            } else {
                                paramIndex++;
                                strWhere += ` AND ${k} ${c['operator']} ?`;
                                arrValues.push(c['value']);
                            }
                        }
                    }
                }
            }
        });

        if (strWhere.length > 0) {
            strWhere = " where " + strWhere.slice(5);
        }

        return [strWhere, arrValues];
    }

    async select({ table, columns = ["*"], where, limit = config.DATA_FETCH_LIMIT, orderBy = [] }) {

        try {
            let strColumn = Array.isArray(columns) ? columns.join() : null;
            if (!strColumn) {
                throw new Error('Invalid columns');
            }

            const [strWhere, arrValues] = Db.conditionsToString(where, this.conn.databaseType);
            const strOrder = orderBy.length > 0 ? ' order by ' + orderBy.join() + ' ' : '';
            const strLimit = limit === 0 ? '' : ' limit ' + limit;

            const sql = "select " + strColumn + " from " + table + strWhere + strOrder + strLimit;
            return await this.query(sql, arrValues);
        }
        catch (error) {
            return new Promise((resolve, reject) => {
                reject(error);
            });
        }
    }

    async query(sql, arrValues) {
        switch (this.conn.databaseType) {
            case 'mysql':
                return new Promise((resolve, reject) => {
                    // execute will internally call prepare and query while query will not call prepare
                    logger.debug(`Select: ${sql}`);
                    this.conn.execute(sql, arrValues, (error, results, fields) => {
                        if (error) {
                            reject(error);
                        } else {
                            logger.info(`Select completed successfully with ${results.length} rows returned`)
                            resolve({ data: results, columns: fields, count: results.length });
                        }
                    });
                });
            case 'postgres':
            case 'postgresql':
                try {
                    // prepare is not recommended for pg library
                    logger.info(`Select: ${sql}`);
                    const res = await this.conn.query(sql, arrValues);
                    return new Promise((resolve, reject) => {
                        logger.info(`Select completed successfully with ${res.rowCount} rows returned`)
                        resolve({ data: res.rows, columns: res.fields, count: res.rowCount });
                    });
                } catch (error) {
                    return new Promise((resolve, reject) => {
                        reject(error);
                    });
                }
            default:
                return new Promise((resolve, reject) => {
                    reject(new Error(`Unsupported database type: ${this.conn.databaseType}`));
                });
        }
    }

}

export default Db;