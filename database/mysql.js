import Db from './db.js';
import logger from '../logger.js';


class MysqlDb extends Db {

    async query(sql, arrValues = []) {
        logger.debug(`Query: ${sql}`);
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
    }
}

export default MysqlDb;