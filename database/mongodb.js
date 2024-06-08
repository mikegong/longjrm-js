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
                logger.info(`Query completed successfully with ${res.length} documents returned`)
                resolve({ data: res, count: res.length });
            });
        } catch (error) {
            return new Promise((resolve, reject) => {
                reject(error);
            });

        }
    }

    async select({ table, columns = ["*"], where, options = { "limit": config.DATA_FETCH_LIMIT, "orderBy": [] } }) {
        // order by format: [column1 asc, column2 desc]
        try {
            const findQuery = this.selectConstructor({ columns, where, options });
            let res = await this.query(findQuery, [], table);
            res.columns = columns;
            return res;
        }
        catch (error) {
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

}

export default MongoDb;

