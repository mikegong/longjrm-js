import Db from './db.js';
import logger from '../logger.js';


class MysqlDb extends Db {
    async query(sql, arrValues = []) {
        logger.debug(`Executing SQL Query: ${sql} with values: ${arrValues.join(', ')}`);
        return new Promise((resolve, reject) => {
            this.conn.query(sql, arrValues, (error, results, fields) => {
                if (error) {
                    logger.error(`MySQL query error: ${error.message}`);
                    reject({ success: false, message: error.message });
                    return;
                }
                
                const isSelectQuery = /^select\s+/i.test(sql.trim());
                const isInsertQuery = /^insert\s+/i.test(sql.trim());
                const affectedRows = results.affectedRows || results.length || 0;
                const successMsg = affectedRows > 0 ? 'Query executed successfully' : 'No rows affected';

                if (isSelectQuery) {
                    const columnNames = fields.map(f => f.name);
                    logger.info(`Query executed successfully. '${sql}' returned ${results.length} row(s).`);
                    resolve({
                        success: true,
                        message: 'Query executed successfully',
                        data: results,
                        columns: columnNames,
                        count: results.length
                    });
                } else {
                    logger.info(`Query executed successfully. '${sql}' affected ${affectedRows} row(s)`);
                    const result = {
                        success: affectedRows > 0,
                        message: successMsg,
                    };

                    if (isInsertQuery) {
                        const columnNamesMatch = sql.match(/\(([^)]+)\)/);
                        const columnNames = columnNamesMatch ? columnNamesMatch[1].split(',').map(c => c.trim()) : [];
                        const insertedData = { id: results.insertId };

                        columnNames.forEach((col, index) => {
                            insertedData[col] = arrValues[index];
                        });

                        result.data = [insertedData];
                        result.columns = ['id', ...columnNames];
                        result.count = affectedRows;
                    } else if (/^update\s+/i.test(sql.trim())) {
                        result.modifiedCount = affectedRows;
                    } else if (/^delete\s+/i.test(sql.trim())) {
                        result.deletedCount = affectedRows;
                    }

                    resolve(result);
                }
            });
        }).catch(error => {
            return new Promise((resolve, reject) => {
                reject(error); 
            });
        });
    }
}

export default MysqlDb;