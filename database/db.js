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

    static simpleConditionParser(condition, paramIndex, databaseType = '') {
        // Simple condition format is as below，
        // input: {column: value}
        // output: [arrCond, arrValues, paramIndex]
        // Please be aware that the condition object here contains only one key/value pair

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
                // Y is default for placeholder
                paramIndex++;
                arrValues.push(cleanValue);
                if (['postgres', 'postgresql'].includes(databaseType)) {
                    // postgresql uses $1, $2, etc. for placeholder
                    arrCond.push(`${column} = $${paramIndex}`);
                } else {
                    arrCond.push(`${column} = ?`);
                }
            }
        } else {
            // Y is default for placeholder
            paramIndex++;
            arrValues.push(value);
            if (['postgres', 'postgresql'].includes(databaseType)) {
                // postgresql uses $1, $2, etc. for placeholder
                arrCond.push(`${column} = $${paramIndex}`);
            } else {
                arrCond.push(`${column} = ?`);
            }
        }

        return [arrCond, arrValues, paramIndex];
    }

    static regularConditionParser(condition, paramIndex, databaseType = '') {
        // Regular condition format is as below，
        // input: {column: {operator1: value1, operator2: value2}}
        // output: [arrCond, arrValues, paramIndex]
        // Please be aware that the object of condition value here supports multiple key/value pairs(operators)

        const column = Object.keys(condition)[0];
        const condObj = Object.values(condition)[0];
        let arrCond = [];
        let arrValues = [];

        for (const [operator, value] of Object.entries(condObj)) {
            if (typeof value === 'string') {
                // escape single quote
                const cleanValue = value.replace("''", "'");
                if (Db.checkCurrentKeyword(cleanValue)) {
                    // CURRENT keyword cannot be put in placeholder
                    arrCond.push(`${column} ${operator} ${Db.unescapeCurrentKeyword(cleanValue)}`);
                } else {
                    // Y is default for placeholder
                    paramIndex++;
                    arrValues.push(cleanValue);
                    if (['postgres', 'postgresql'].includes(databaseType)) {
                        // postgresql uses $1, $2, etc. for placeholder
                        arrCond.push(`${column} ${operator} $${paramIndex}`);
                    } else {
                        arrCond.push(`${column} ${operator} ?`);
                    }
                }
            } else {
                // Y is default for placeholder
                paramIndex++;
                arrValues.push(value);
                if (['postgres', 'postgresql'].includes(databaseType)) {
                    // postgresql uses $1, $2, etc. for placeholder
                    arrCond.push(`${column} ${operator} $${paramIndex}`);
                } else {
                    arrCond.push(`${column} ${operator} ?`);
                }
            }
        }

        return [arrCond, arrValues, paramIndex];
    }

    static comprehensiveConditionParser(condition, paramIndex, databaseType = '') {
        // Comprehensive condition format is as below，
        // {column: {"operator": ">", "value": value, "placeholder": "N"}}
        // Please be aware that the object of condition value contains only one filter operator

        const column = Object.keys(condition)[0];
        const condObj = Object.values(condition)[0];
        const operator = condObj['operator'];
        const value = condObj['value'];
        let arrCond = [];
        let arrValues = [];

        if (typeof value === 'string') {
            // escape single quote
            const cleanValue = value.replace("''", "'");
            if (Db.checkCurrentKeyword(cleanValue)) {
                // CURRENT keyword cannot be put in placeholder
                arrCond.push(`${column} ${operator} ${Db.unescapeCurrentKeyword(cleanValue)}`);
            } else {
                if (condObj['placeholder'] === 'N') {
                    arrCond.push(`${column} ${operator} '${cleanValue}'`);
                } else {
                    // Y is default for placeholder
                    paramIndex++;
                    arrValues.push(cleanValue);
                    if (['postgres', 'postgresql'].includes(databaseType)) {
                        // postgresql uses $1, $2, etc. for placeholder
                        arrCond.push(`${column} ${operator} $${paramIndex}`);
                    } else {
                        arrCond.push(`${column} ${operator} ?`);
                    }
                }
            }
        } else {
            if (condObj['placeholder'] === 'N') {
                arrCond.push(`${column} ${operator} ${value}`);
            } else {
                // Y is default for placeholder
                paramIndex++;
                arrValues.push(value);
                if (['postgres', 'postgresql'].includes(databaseType)) {
                    // postgresql uses $1, $2, etc. for placeholder
                    arrCond.push(`${column} ${operator} $${paramIndex}`);
                } else {
                    arrCond.push(`${column} ${operator} ?`);
                }
            }
        }

        return [arrCond, arrValues, paramIndex];
    }

    static operatorConditionParser(condition, paramIndex, databaseType = '') {

    }

    static whereParser(where, databaseType = '') {
        /*
        where is an array of JSON objects that define query conditions.
            [{condition1}, {condition2}]
        Conditions can be simple, regular, comprehensive, or logical operators.
        1. Simple condition 
            {column: value}, which is equivalent to regular condition {column: {"=": value}} and comprehensive condition {column: {"operator": "=", "value": value, "placeholder": "Y"}}
        2. Regular condition 
            {column: {operator1: value1, operator2: value2}}
        3. Comprehensive condition 
            {column: {"operator": ">", "value": value, "placeholder": "N"}}
        4. Logical operators
            [$operator: [{column1: value1}, {column2: value2}]]
        
            Note: placeholder is optional and default to Y. If placeholder is N, then the value will be put in the query statement directly without using placeholder.
        */

        let arrCond = [];
        let arrValues = [];
        let parsedCond = [];
        let parsedValues = [];
        let paramIndex = 0;

        try {
            if (where.length === 0) {
                logger.debug('Where condition is empty');
                return ['', []];
            }

            where.forEach(condition => {
                if (condition) {
                    if (typeof Object.values(condition)[0] === 'string') {
                        [arrCond, arrValues, paramIndex] = Db.simpleConditionParser(condition, paramIndex, databaseType);
                    } else if (typeof Object.values(condition)[0] === "object") {
                        const keys = Object.keys(Object.values(condition)[0]);
                        if (keys.includes("operator")
                            && keys.includes("value")
                            && keys.includes("placeholder")) {
                            [arrCond, arrValues, paramIndex] = Db.comprehensiveConditionParser(condition, paramIndex, databaseType);
                        }
                        else {
                            [arrCond, arrValues, paramIndex] = Db.regularConditionParser(condition, paramIndex, databaseType);
                        }
                    } else if (Object.keys(condition)[0].startsWith('$') && Object.values(condition)[0].length > 0) {
                        [arrCond, arrValues, paramIndex] = Db.operatorConditionParser(condition, paramIndex, databaseType);
                    } else {
                        throw new Error('Invalid where condition');
                    }
                    parsedCond.push(...arrCond);
                    arrValues !== null ? parsedValues.push(...arrValues) : null;
                }
            });
        } catch (error) {
            logger.error(error);
            throw error;
        }

        return [' where ' + parsedCond.join(' and '), parsedValues];
    }

    async select({ table, columns = ["*"], where, options = { "limit": config.DATA_FETCH_LIMIT, "orderBy": [] } }) {
        // order by format: [column1 asc, column2 desc]
        try {
            if (this.conn.databaseType === 'mongodb' || this.conn.databaseType === 'mongodb+srv') {
                const findQuery = this.mongoSelectConstructor({ columns, where, options });
                let res = await this.query(findQuery, [], table);
                res.columns = columns;
                return res;
            } else {
                const [selectQuery, arrValues] = this.selectConstructor({ table, columns, where, options })
                return await this.query(selectQuery, arrValues);
            }
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
        logger.debug(`Select: ${sql}`);
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
                        logger.info(`Query completed successfully with ${res.length} rows returned`)
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