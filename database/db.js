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
                    if (typeof Object.values(condition)[0] != 'object') {
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

    async query(sql, arrValues = [], collectionName = null) {
        logger.debug(`Query: ${sql}`);
        supportedDatabaseType = ['mysql', 'postgres', 'postgresql', 'mongodb', 'mongodb+srv'];
        if (!supportedDatabaseType.includes(this.conn.databaseType)) {
            return new Promise((resolve, reject) => {
                reject(new Error(`Unsupported database type: ${this.conn.databaseType}`));
            });
        }
    }

}

export default Db;