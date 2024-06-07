/*
Description: 
JRM - This is a generic JSON Relational Mapping library that wrap crud sql statements via database api.
conn - database connection
table - the table that is populated
data - json data including column and value pairs
where condition - json data that defines where column and value pairs
*/

import logger from "../logger.js";
import { config } from '../load-config.js';

class Db {
    constructor(conn) {
        this.conn = conn;
        this.databaseType = this.conn.type;
    }

    static formatPlaceholder(paramIndex, databaseType) {
        return ['postgres', 'postgresql'].includes(databaseType) ? `$${paramIndex}` : '?';
    }
    
    static checkCurrentKeyword(string) {
        /*
         check if string contains reserved CURRENT SQL keyword below
            CURRENT DATE, CURRENT_DATE, CURRENT TIMESTAMP, CURRENT_TIMESTAMP
         1. If CURRENT keyword is found with prefix escape ` and suffix escape ` without escape character \\,
            then that is keyword and return True
        */

        const keywords = ['`CURRENT DATE`', '`CURRENT_DATE`', '`CURRENT TIMESTAMP`', '`CURRENT_TIMESTAMP`'];
        const upperString = string.toUpperCase();

        for (let keyword of keywords) {
            if (upperString.includes(keyword) && !upperString.includes(`\\${keyword}\\`)) {
                return true;
            }
        }
    
        return false;
    }

    static caseInsensitiveReplace(str, search, replacement) {
        const regex = new RegExp(search, 'gi'); // 'g' for global, 'i' for insensitive
        return str.replace(regex, replacement);
    }

    static unescapeCurrentKeyword(string) {
        /*
         unescape reserved CURRENT SQL keyword below, which is quoted by `
            CURRENT DATE, CURRENT_DATE, CURRENT TIMESTAMP, CURRENT_TIMESTAMP
        */

        const keywords = ['CURRENT DATE', 'CURRENT_DATE', 'CURRENT TIMESTAMP', 'CURRENT_TIMESTAMP'];

        for (let keyword of keywords) {
            string = Db.caseInsensitiveReplace(string, `\`${keyword}\``, keyword);
        }

        return string;
    }

    static simpleConditionParser(condition, paramIndex, databaseType = '') {
        const column = Object.keys(condition)[0];
        const value = Object.values(condition)[0];
        let arrCond = [];
        let arrValues = [];
    
        if (typeof value === 'string') {
            // escape single quote
            const cleanValue = value.replace("''", "'");
            if (Db.checkCurrentKeyword(cleanValue)) {
                // CURRENT keyword cannot be put in placeholder
                arrCond.push(`${column} = ${Db.unescapeCurrentKeyword(cleanValue)}`);
            } else {
                paramIndex++;
                arrValues.push(cleanValue);
                arrCond.push(`${column} = ${Db.formatPlaceholder(paramIndex, databaseType)}`);
            }
        } else {
            paramIndex++;
            arrValues.push(value);
            arrCond.push(`${column} = ${Db.formatPlaceholder(paramIndex, databaseType)}`);
        }
        return [arrCond, arrValues, paramIndex];
    }
    
    static regularConditionParser(condition, paramIndex, databaseType = '') {
        const column = Object.keys(condition)[0];
        const condObj = Object.values(condition)[0];
        let arrCond = [];
        let arrValues = [];
    
        for (const [operator, value] of Object.entries(condObj)) {
            if (typeof value === 'string') {
                const cleanValue = value.replace("''", "'");
                if (Db.checkCurrentKeyword(cleanValue)) {
                    arrCond.push(`${column} ${operator} ${Db.unescapeCurrentKeyword(cleanValue)}`);
                } else {
                    paramIndex++;
                    arrValues.push(cleanValue);
                    arrCond.push(`${column} ${operator} ${Db.formatPlaceholder(paramIndex, databaseType)}`);
                }
            } else {
                paramIndex++;
                arrValues.push(value);
                arrCond.push(`${column} ${operator} ${Db.formatPlaceholder(paramIndex, databaseType)}`);
            }
        }
        return [arrCond, arrValues, paramIndex];
    }
    
    static comprehensiveConditionParser(condition, paramIndex, databaseType = '') {
        const column = Object.keys(condition)[0];
        const condObj = Object.values(condition)[0];
        const operator = condObj['operator'];
        const value = condObj['value'];
        let arrCond = [];
        let arrValues = [];
    
        if (typeof value === 'string') {
            const cleanValue = value.replace("''", "'");
            if (Db.checkCurrentKeyword(cleanValue)) {
                arrCond.push(`${column} ${operator} ${Db.unescapeCurrentKeyword(cleanValue)}`);
            } else {
                if (condObj['placeholder'] === 'N') {
                    arrCond.push(`${column} ${operator} '${cleanValue}'`);
                } else {
                    paramIndex++;
                    arrValues.push(cleanValue);
                    arrCond.push(`${column} ${operator} ${Db.formatPlaceholder(paramIndex, databaseType)}`);
                }
            }
        } else {
            if (condObj['placeholder'] === 'N') {
                arrCond.push(`${column} ${operator} ${value}`);
            } else {
                paramIndex++;
                arrValues.push(value);
                arrCond.push(`${column} ${operator} ${Db.formatPlaceholder(paramIndex, databaseType)}`);
            }
        }
        return [arrCond, arrValues, paramIndex];
    }
    
    static operatorConditionParser(condition, paramIndex, databaseType = '') {
        const operator = Object.keys(condition)[0];
        const conditions = Object.values(condition)[0];
        let arrCond = [];
        let arrValues = [];
    
        const logicalOperators = {
            '$and': 'AND',
            '$or': 'OR',
            '$not': 'NOT'
        };
    
        if (!logicalOperators[operator]) {
            throw new Error(`Unsupported logical operator: ${operator}`);
        }
    
        conditions.forEach(subCondition => {
            const [subCond, subValues, newParamIndex] = Db.whereParser([subCondition], databaseType, paramIndex);
            arrCond.push(`(${subCond})`);
            arrValues.push(...subValues);
            paramIndex = newParamIndex;
        });
    
        const combinedCond = arrCond.join(` ${logicalOperators[operator]} `);
        return [combinedCond, arrValues, paramIndex];
    }
    
    static whereParser(where, databaseType = '', startIndex = 0) {
        let arrCond = [];
        let arrValues = [];
        let parsedCond = [];
        let parsedValues = [];
        let paramIndex = startIndex;
    
        try {
            if (where.length === 0) {
                logger.debug('Where condition is empty');
                return ['', []];
            }
    
            where.forEach(condition => {
                if (condition) {
                    if (typeof Object.values(condition)[0] != 'object') {
                        [arrCond, arrValues, paramIndex] = Db.simpleConditionParser(condition, paramIndex, databaseType);
                    } else if (typeof Object.values(condition)[0] === "object") {
                        const keys = Object.keys(Object.values(condition)[0]);
                        if (keys.includes("operator") && keys.includes("value") && keys.includes("placeholder")) {
                            [arrCond, arrValues, paramIndex] = Db.comprehensiveConditionParser(condition, paramIndex, databaseType);
                        } else {
                            [arrCond, arrValues, paramIndex] = Db.regularConditionParser(condition, paramIndex, databaseType);
                        }
                    } else if (Object.keys(condition)[0].startsWith('$') && Object.values(condition)[0].length > 0) {
                        [arrCond, arrValues, paramIndex] = Db.operatorConditionParser(condition, paramIndex, databaseType);
                    } else {
                        throw new Error('Invalid where condition');
                    }
                    parsedCond.push(...arrCond);
                    parsedValues.push(...arrValues);
                }
            });
    
        } catch (error) {
            logger.error(error);
            throw error;
        }
    
        const whereClause = parsedCond.length > 0 ? ' WHERE ' + parsedCond.join(' AND ') : '';
        return [whereClause, parsedValues, paramIndex];
    }
    
    async select({ table, columns = ["*"], where, options = { "limit": config.DATA_FETCH_LIMIT, "orderBy": [] } }) {
        // order by format: [column1 asc, column2 desc]
        try {
            const [selectQuery, arrValues] = this.selectConstructor({ table, columns, where, options })
            return await this.query(selectQuery, arrValues);
        }
        catch (error) {
            return new Promise((resolve, reject) => {
                reject(error);
            });
        }
    }

    selectConstructor({ table, columns = ["*"], where, options = { "limit": config.DATA_FETCH_LIMIT, "orderBy": [] } }) {
        let strColumn = Array.isArray(columns) ? columns.join() : null;
        if (!strColumn) {
            throw new Error('Invalid columns');
        }

        const [strWhere, arrValues] = Db.whereParser(where, this.conn.databaseType);
        const strOrder = options.orderBy.length > 0 ? ' order by ' + options.orderBy.join() + ' ' : '';
        const strLimit = options.limit === 0 ? '' : ' limit ' + options.limit;

        const selectQuery = "select " + strColumn + " from " + table + strWhere + strOrder + strLimit;
        return [selectQuery, arrValues];
    }

    mongoSelectConstructor({ columns = ["*"], where, options = { "limit": config.DATA_FETCH_LIMIT, "orderBy": [] } }) {
        let projection = {};
        if (!(columns.length === 1 && columns[0] === '*')) {
            projection = columns.reduce((obj, item) => {
                obj[item] = 1;
                return obj;
            }, {})
        }
        let selectQuery = {
            query: where,
            options: {
                limit: options.limit,
                sort: options.orderBy.reduce((obj, item) => {
                    const [column, order] = item.split(' ');
                    obj[column] = order === 'desc' ? -1 : 1;  // default to asc
                    return obj;
                }, {}),
                projection: projection,
            }
        };
        return selectQuery;
    }

    async query(sql, arrValues=[], collectionName=null) {
        // collection_name is used for MongoDb only
        logger.debug(`Query: ${sql}`);
        switch (this.conn.databaseType) {
            case 'mysql':
                return new Promise((resolve, reject) => {
                    // execute will internally call prepare and query while query will not call prepare
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
            case 'mongodb':
            case 'mongodb+srv':
                try {
                    const {query, options} = sql;
                    const res = await this.conn.db(this.conn.databaseName).collection(collectionName).find(query, options).toArray();
                    return new Promise((resolve, reject) => {
                        logger.info(`Query completed successfully with ${res.length} documents returned`)
                        resolve({ data: res, count: res.length });
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