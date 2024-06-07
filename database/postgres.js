import Db from './db.js';
import logger from '../logger.js';

class PostgresDb extends Db {
    constructor(conn) {
        super(conn);
        this.databaseType = 'postgres';
    }

    async query(sql, arrValues = []) {
        logger.debug(`Executing Query: ${sql} with values: ${arrValues.join(', ')}`);
        try {
            const res = await this.conn.query(sql, arrValues);
            logger.info(`Query executed successfully. '${sql}' affected ${res.rowCount} row(s).`);
    
            // Prepare the result object
            const result = {
                success: true,
                message: 'Query executed successfully',  
            };
    
            // Check if the query is an INSERT or SELECT operation
            if (/^insert\s+/i.test(sql.trim()) || /^select\s+/i.test(sql.trim())) {
                result.data = res.rows;
                result.columns = res.fields ? res.fields.map(f => f.name) : [];
                result.count = res.rowCount
            }
    
            // Check if the query is an UPDATE or DELETE operation to set modifiedCount or deletedCount
            if (/^update\s+/i.test(sql.trim())) {
                result.modifiedCount = res.rowCount;
            } else if (/^delete\s+/i.test(sql.trim())) {
                result.deletedCount = res.rowCount;
            }
            return result;
        } catch (error) {
            return new Promise((resolve, reject) => {
                reject(error); 
            });
        }
    }
    
}    

export default PostgresDb;