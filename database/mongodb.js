import Db from './db.js';
import logger from '../logger.js';
import { config } from '../load-config.js';

class MongoDb extends Db {

    async query(sql, arrValues = [], collectionName = null) {
        logger.debug(`Query: ${sql}`);
        try {
            const {query, options} = sql;
            const res = await this.conn.db(this.conn.databaseName).collection(collectionName).find(query, options).toArray();
            return new Promise((resolve, reject) => {
                resolve({ data: res, count: res.length });
            });
        } catch (error) {
            return new Promise((resolve, reject) => {
                reject(error);
            });
        }
    }

    async select({ table, columns = ["*"], where, options = { "limit": config.DATA_FETCH_LIMIT, "orderBy": [] } }) {
        try {
            const findQuery = this.selectConstructor({ columns, where, options });
            let res = await this.query(findQuery, [], table);
            res.columns = columns;
            
            if (res.data.length > 0) {
                logger.info(`MongoDB select operation completed successfully with ${res.data.length} document(s) found`);
            } else {
                logger.info(`MongoDB select operation completed with no documents found`);
            }
            return res;
        } catch (error) {
            return new Promise((resolve, reject) => {
                reject(error);
            });
        }
    }

    selectConstructor({ columns = ["*"], where, options = { "limit": config.DATA_FETCH_LIMIT, "orderBy": [] } }) {
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

    async insert({ table, data }) {
        try {
            let collection = this.conn.db().collection(table);
            let res;
            if (Array.isArray(data)) {
                res = await collection.insertMany(data);
            } else {
                res = await collection.insertOne(data);
            }

            if (res.acknowledged) {
                let insertedDocuments = [];
                if (res.hasOwnProperty('insertedId')) { // Result from insertOne
                    // Fetch the inserted document
                    insertedDocuments = [await collection.findOne({ _id: res.insertedId })];
                } else { // Result from insertMany
                    // Fetch all inserted documents
                    const ids = Object.values(res.insertedIds);
                    insertedDocuments = await collection.find({ _id: { $in: ids } }).toArray();
                }

                logger.info(`MongoDB insert operation completed successfully with ${insertedDocuments.length} document(s) affected`);
                return {
                    success: true,
                    message: 'Data inserted successfully',
                    data: insertedDocuments,
                    count: insertedDocuments.length
                };
            } else {
                logger.info('MongoDB insert operation failed to acknowledge');
                return { success: false, message: 'Insert operation failed' };
            }
        } catch (error) {
            return new Promise((resolve, reject) => {
                reject(error);
            });
        }
    }

    insertConstructor({ data, options }) {
        return {
            query: { insert: data },
            options: options
        };
    }

    async update({ table, data, where = {}, options = {} }) {
        const collection = this.conn.db().collection(table);
        try {
            const res = await collection.updateMany(where, { $set: data }, options);
            if (res.modifiedCount > 0) {
                logger.info(`MongoDB update operation completed successfully with ${res.modifiedCount} document(s) affected on ${table}`);
            } else {
                logger.info(`MongoDB update operation completed with no documents affected on ${table}`);
            }
            return {
                success: res.modifiedCount > 0,
                modifiedCount: res.modifiedCount,
                message: 'Data updated successfully'
            };
        } catch (error) {
            return new Promise((resolve, reject) => {
                reject(error);
            });
        }
    }

    updateConstructor({ data, where, options }) {
        // The $set operator is used to update the fields in the documents.
        return {
            query: where[0], // Assuming where is an array of one or more conditions, using the first one here
            update: { $set: data },
            options: options
        };
    }

    async delete({ table, where = {}, options = {} }) {
        const collection = this.conn.db().collection(table);
        try {
            const res = await collection.deleteMany(where, options);
            if (res.deletedCount > 0) {
                logger.info(`MongoDB delete operation completed successfully with ${res.deletedCount} document(s) removed from ${table}`);
            } else {
                logger.info(`MongoDB delete operation completed with no documents removed from ${table}`);
            }
            return {
                success: res.deletedCount > 0,
                deletedCount: res.deletedCount,
                message: 'Data deleted successfully'
            };
        } catch (error) {
            return new Promise((resolve, reject) => {
                reject(error);
            });
        }
    }

    deleteConstructor({ where, options }) {
        // Construct the delete operation with the appropriate filtering.
        return {
            query: where[0], // Assuming where is an array of one or more conditions, using the first one here
            options: options
        };
    }
}

export default MongoDb;

