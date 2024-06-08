import Db from './db.js';
import logger from '../logger.js';


class PostgresDb extends Db {

    async query(sql, arrValues = []) {
        logger.debug(`Query: ${sql}`);
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
    }
}

export default PostgresDb;
