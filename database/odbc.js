import Db from './db.js';
import logger from '../logger.js';


class OdbcDb extends Db {

    async query(sql, arrValues = []) {
        logger.debug(`Query: ${sql}`);
        try {
            const statement = await this.conn.createStatement();
            await statement.prepare(sql);
            await statement.bind(arrValues);
            const result = await statement.execute();
            const rows = []
            result.forEach(row => {
                rows.push(row);
            });
            const columns = result.columns.map(column => column.name);
            const count = rows.length;

            return new Promise((resolve, reject) => {
                logger.info(`Select completed successfully with ${count} rows returned`)
                resolve({ data: rows, columns: columns, count: count });
            });
        } catch (error) {
            return new Promise((resolve, reject) => {
                reject(error);
            });
        }
    }
}

export default OdbcDb;