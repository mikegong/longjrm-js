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

    constructSqlWhere(conditions, startIndex = 1) {
        if (!conditions || conditions.length === 0) return { clause: '', values: [] };
    
        let values = [];
        let clause = conditions.map((condition, index) => {
            const key = Object.keys(condition)[0];
            const value = condition[key];
            values.push(value);
            const placeholder = this.conn.databaseType.includes('postgres') ? `$${startIndex + index}` : '?';
            return `${key} = ${placeholder}`;
        }).join(' AND ');
    
        return { clause, values };
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

    async select({ table, columns = ["*"], where, options = { "limit": config.DATA_FETCH_LIMIT, "orderBy": [], "page": 1, "pageSize": 1000 }  }) {
        try {
            const offset = (options.page - 1) * options.pageSize;
            const limit = options.pageSize;

            if (this.conn.databaseType.includes('mongodb')) {
                const formattedWhere = Array.isArray(where) ? where[0] : where;
                const findQuery = this.mongoSelectConstructor({ columns, where: formattedWhere, options: {...options, limit, offset} });
                let res = await this.query(findQuery, [], table);
                res.columns = columns;
                if (res.data.length > 0) {
                    logger.info(`MongoDB select operation completed successfully with ${res.data.length} documents found`);
                } else {
                    logger.info(`MongoDB select operation completed with no documents found`);
                }
                return res;
            } else {
                const [selectQuery, arrValues] = this.selectConstructor({ table, columns, where, options: {...options, limit, offset} });
                const res = await this.query(selectQuery, arrValues);
                res.count = res.data.length;  // Set count based on the data array length
                return {
                    success: true,
                    data: res.data,
                    columns: res.columns,
                    count: res.count,
                    message: 'Query executed successfully'
                };
            }
        } catch (error) {
            return {
                success: false,
                data: [],
                columns: [],
                count: 0,
                message: error.message
            };
        }
    }
    
    selectConstructor({ table, columns = ["*"], where, options }) {
        let strColumn = Array.isArray(columns) ? columns.join() : null;
        if (!strColumn) {
            throw new Error('Invalid columns');
        }

        const [strWhere, arrValues] = Db.whereParser(where, this.conn.databaseType);
        const strOrder = options.orderBy.length > 0 ? ' order by ' + options.orderBy.join() + ' ' : '';
        const strLimit = options.limit ? ' limit ' + options.limit : ' limit 1000';  // default limit
        const strOffset = options.offset ? ' offset ' + options.offset : ' offset 0'; // default offset

        const selectQuery = `select ${strColumn} from ${table}${strWhere}${strOrder}${strLimit}${strOffset}`;
        return [selectQuery, arrValues];
    }

    mongoSelectConstructor({ columns = ["*"], where, options = { "limit": config.DATA_FETCH_LIMIT, "orderBy": [], "page": 1, "pageSize": 1000} }) {
        let projection = {};
        if (!(columns.length === 1 && columns[0] === '*')) {
            projection = columns.reduce((obj, item) => {
                obj[item] = 1;
                return obj;
            }, {})
        }

        let skip = (options.page - 1) * options.pageSize; // Calculate the number of documents to skip
        let limit = options.pageSize; // Number of documents to return

        let selectQuery = {
            query: where,
            options: {
                limit: limit,
                skip: skip,
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

    

    async insert({ table, data }) {
        try {
            if (this.conn.databaseType.includes('mongodb')) {
                let collection = this.conn.db().collection(table);
            let insertResult;
            if (Array.isArray(data)) {
                insertResult = await collection.insertMany(data);
            } else {
                insertResult = await collection.insertOne(data);
            }

            if (!insertResult.acknowledged) {
                return { success: false, message: 'Insert operation failed' };
            }

            let insertedDocuments = [];
            if (insertResult.hasOwnProperty('insertedId')) { // Result from insertOne
                // Fetch the inserted document
                insertedDocuments = [await collection.findOne({_id: insertResult.insertedId})];
            } else { // Result from insertMany
                // Fetch all inserted documents
                const ids = Object.values(insertResult.insertedIds);
                insertedDocuments = await collection.find({_id: {$in: ids}}).toArray();
            }

            logger.info(`MongoDB insert operation completed successfully with ${insertedDocuments.length} documents affected`);
            return {
                success: true,
                message: 'Data inserted successfully',
                data: insertedDocuments,
                count: insertedDocuments.length
            };
            } else {
                const [insertQuery, arrValues] = this.sqlInsertConstructor({ table, data, databaseType: this.conn.databaseType });
                const insertResult = await this.query(insertQuery, arrValues);
                if (!insertResult.success) {
                    return insertResult;
                }
    
                if (insertResult.data && insertResult.data.length > 0) {
                      return {
                          success: true,
                          message: 'Data inserted successfully',
                          data: insertResult.data[0],
                          columns: Object.keys(insertResult.data[0]),
                          count: insertResult.data.length
                        };
                } else {
                    // Handle SQL databases without RETURNING clause
                    if (insertResult.affectedRows > 0) {
                        const selectQuery = `SELECT * FROM ${table} WHERE id = LAST_INSERT_ID()`;  // MySQL-specific
                        const selectResult = await this.query(selectQuery);
                        if (selectResult.success && selectResult.data.length > 0) {
                            return {
                                success: true,
                                data: selectResult.data[0],
                                columns: selectResult.columns,
                                count: selectResult.data.length,
                                message: 'Data inserted successfully'
                            };
                        } else {
                            return {
                                success: false,
                                message: 'Failed to retrieve inserted data',
                                data: [],
                                count: 0
                            };
                        }
                    }
                }
            }
        } catch (error) {
            return { success: false, message: error.message };
        }
    }
    
    sqlInsertConstructor({ table, data, databaseType }) {
        const columns = Object.keys(data);
        const values = Object.values(data);
        const placeholders = columns.map((_, index) => databaseType.includes('postgres') ? `$${index + 1}` : '?').join(', ');
    
        // Use 'let' instead of 'const' to allow modification later
        let insertQuery = `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`;
    
        if (databaseType.includes('postgres')) {
            // PostgreSQL supports the RETURNING clause, which can return the newly inserted row.
            insertQuery += ' RETURNING *';
        }
    
        return [insertQuery, values];
    }
    
    mongoInsertConstructor({ data, options }) {
        return {
            query: { insert: data },
            options: options
        };
    }
    
    
    async update({ table, data, where, options = {} }) {
        try {
            if (!where || !Array.isArray(where) || where.length === 0) {
                return {
                    success: false,
                    message: "Invalid 'where' parameter: Must be a non-empty array."
                };
            }
            if (this.conn.databaseType.includes('mongodb')) {
                if (!this.conn.db) {
                    throw new Error("MongoDB connection not initialized.");
                }
                let collection = this.conn.db().collection(table);
                console.log(`Updating documents in ${table} with criteria:`, where[0], "and data:", data);
                let res = await collection.updateMany(where[0], { $set: data }, options);
            if (res.modifiedCount > 0) {
                logger.info(`MongoDB update operation completed successfully with ${res.modifiedCount} documents affected on ${table}`);
            } else {
                logger.info(`MongoDB update operation completed with no documents affected on ${table}`);
            }
            return {
                success: res.modifiedCount > 0,
                message: 'Data updated successfully',
                modifiedCount: res.modifiedCount
            };
            } else {
                const [updateQuery, arrValues] = this.sqlUpdateConstructor({ table, data, where, databaseType: this.conn.databaseType });
            const updateResult = await this.query(updateQuery, arrValues);
    
            let affectedRows = this.conn.databaseType.includes('postgres') ? updateResult.count : updateResult.affectedRows;
    
            return {
                success: affectedRows > 0,
                message: 'Data updated successfully',
                modifiedCount: affectedRows
            };
        } 
    } catch (error) {
            console.error("Update failed:", error);
            return { success: false, message: error.message };
        }
    }

    sqlUpdateConstructor({ table, data, where, databaseType }) {
        const keys = Object.keys(data);
        const values = Object.values(data);
        const setParts = keys.map((key, index) => `${key} = ${databaseType.includes('postgres') ? `$${index + 1}` : '?'}`);
        const setClause = setParts.join(', ');
        const { clause: whereClause, values: whereValues } = this.constructSqlWhere(where, keys.length + 1, databaseType);
        const updateQuery = `UPDATE ${table} SET ${setClause} WHERE ${whereClause}`;
        return [updateQuery, [...values, ...whereValues]];
    }
    
    
    mongoUpdateConstructor({ data, where, options }) {
        // The $set operator is used to update the fields in the documents.
        return {
            query: where[0], // Assuming where is an array of one or more conditions, using the first one here
            update: { $set: data },
            options: options
        };
    }
    
    
    
    async delete({ table, where, options = {} }) {
        try {
            if (!where || !Array.isArray(where) || where.length === 0) {
                console.error("Invalid 'where' condition provided:", where);
                return {
                    success: false,
                    message: "Invalid 'where' parameter: Must be a non-empty array."
                };
            }
    
            if (this.conn.databaseType.includes('mongodb')) {
                if (!this.conn.db) {
                    throw new Error("MongoDB connection not initialized.");
                }
                let collection = this.conn.db().collection(table);
                console.log(`Deleting documents from ${table} with criteria:`, where[0]);
                let res = await collection.deleteMany(where[0], options);
            if (res.deletedCount > 0) {
                logger.info(`MongoDB delete operation completed successfully with ${res.deletedCount} documents removed from ${table}`);
            } else {
                logger.info(`MongoDB delete operation completed with no documents removed from ${table}`);
            }
            return {
                success: true,
                message: 'Data deleted successfully',
                deletedCount: res.deletedCount
            };
            } else {
                const [deleteQuery, arrValues] = this.sqlDeleteConstructor({ table, where, databaseType: this.conn.databaseType });
                const deleteResult = await this.query(deleteQuery, arrValues);

                let affectedRows = this.conn.databaseType.includes('postgres') ? deleteResult.count : deleteResult.affectedRows;

        return {
            success: affectedRows > 0,
            message: 'Data deleted successfully',
            deletedCount: affectedRows
            };
        } 
    }catch (error) {
        console.error("Delete failed:", error);
        return { success: false, message: error.message };
    }
}
    
    sqlDeleteConstructor({ table, where, databaseType }) {  
        const { clause: whereClause, values: whereValues } = this.constructSqlWhere(where, 1, databaseType);
        const deleteQuery = `DELETE FROM ${table} WHERE ${whereClause}`;
        return [deleteQuery, whereValues];
}
    
    mongoDeleteConstructor({ where, options }) {
        // Construct the delete operation with the appropriate filtering.
        return {
            query: where[0], // Assuming where is an array of one or more conditions, using the first one here
            options: options
        };
    }

    async query(sql, arrValues = [], collectionName = null) {
        logger.debug(`Query: ${sql}`);
        switch (this.conn.databaseType) {
            case 'mysql':
                return new Promise((resolve, reject) => {
                    this.conn.query(sql, arrValues, (error, results, fields) => {
                        if (error) {
                            logger.error(`MySQL query execution error:`, error);
                            reject({ success: false, message: error.message });
                        } else {
                            const sqlType = sql.trim().substring(0, 6).toLowerCase();
                            switch (sqlType) {
                                case 'select':
                                    // Conditionally log only for specific queries
                                    if (sql.includes("limit 1000")) {
                                        logger.info(`Executed SQL: ${sql}, Returned Rows: ${results.length}`);
                                    }
                                    resolve({
                                        success: true,
                                        data: results,
                                        columns: fields ? fields.map(field => field.name) : [],
                                        count: results.length,
                                        message: 'Query executed successfully'
                                    });
                                    break;
                                case 'insert':
                                    logger.info(`Executed SQL: ${sql}, Affected Rows: ${results.affectedRows}, Insert ID: ${results.insertId}`);
                                    resolve({
                                        success: true,
                                        message: 'Data inserted successfully',
                                        data: results.insertId > 0 ? { id: results.insertId } : undefined,
                                        insertId: results.insertId,
                                        affectedRows: results.affectedRows
                                    });
                                    break;
                                default: // This includes 'update' and 'delete'
                                    logger.info(`Executed SQL: ${sql}, Affected Rows: ${results.affectedRows}`);
                                    if (results.affectedRows > 0) {
                                        resolve({
                                            success: true,
                                            message: 'Operation successful',
                                            affectedRows: results.affectedRows
                                        });
                                    } else {
                                        resolve({
                                            success: false,
                                            message: 'No data updated or deleted',
                                            affectedRows: results.affectedRows
                                        });
                                    }
                            }
                        }
                    });
                });
    
            case 'postgres':
            case 'postgresql':
                return new Promise((resolve, reject) => {
                    this.conn.query(sql, arrValues).then(res => {
                        logger.info(`Executed SQL: ${sql} completed successfully with ${res.rowCount} rows returned`);
                        resolve({
                            success: true,
                            data: res.rows,
                            columns: res.fields.map(f => f.name),
                            count: res.rowCount,
                            message: 'Query executed successfully'
                        });
                    }).catch(error => {
                        logger.error(`PostgreSQL query execution error:`, error);
                        reject({ success: false, message: error.message });
                    });
                });

            case 'mongodb':
            case 'mongodb+srv':
                return new Promise((resolve, reject) => {
                    const {query, options} = sql;
                    this.conn.db(this.conn.databaseName).collection(collectionName).find(query, options).toArray().then(res => {
                        resolve({
                            success: true,
                            data: res,
                            count: res.length,
                            message: 'Query executed successfully'
                        });
                    }).catch(error => {
                        logger.error(`MongoDB query execution error:`, error);
                        reject({ success: false, message: error.message });
                    });
                });

            default:
                return new Promise((resolve, reject) => {
                    reject(new Error(`Unsupported database type: ${this.conn.databaseType}`));
                });
        }
    }
}
export default Db;