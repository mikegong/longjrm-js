import Db from './db.js';  

class DbPostgres extends Db {
    constructor(conn) {
        super(conn);
    }

    async insert(table, data) {
        const fields = Object.keys(data);
        const values = Object.values(data);
        const placeholders = fields.map((_, index) => `$${index + 1}`).join(', ');
        
        const sql = `INSERT INTO ${table} (${fields.join(', ')}) VALUES (${placeholders}) RETURNING *;`;
        return new Promise((resolve, reject) => {
            this.conn.query(sql, values, (error, results) => {
                if (error) reject(error);
                else resolve(results.rows);  
            });
        });
    }

    async update(table, data, whereCondition) {
        const whereKey = Object.keys(whereCondition)[0];
        const whereValue = whereCondition[whereKey];
        const updateFields = Object.keys(data).map((key, idx) => `${key} = $${idx + 1}`).join(', ');
        
        const sql = `UPDATE ${table} SET ${updateFields} WHERE ${whereKey} = $${Object.keys(data).length + 1} RETURNING *;`;
        const values = [...Object.values(data), whereValue];

        return new Promise((resolve, reject) => {
            this.conn.query(sql, values, (error, results) => {
                if (error) reject(error);
                else resolve(results.rows);  // Assuming results.rows contains the updated rows
            });
        });
    }

    async delete(table, whereCondition) {
        const whereKey = Object.keys(whereCondition)[0];
        const whereValue = whereCondition[whereKey];
        const sql = `DELETE FROM ${table} WHERE ${whereKey} = $1 RETURNING *;`;

        return new Promise((resolve, reject) => {
            this.conn.query(sql, [whereValue], (error, results) => {
                if (error) reject(error);
                else resolve(results.rows);  
            });
        });
    }

    async selectAll(table) {
        const sql = `SELECT * FROM ${table};`;
        return new Promise((resolve, reject) => {
            this.conn.query(sql, (error, results) => {
                if (error) reject(error);
                else resolve(results.rows);  
            });
        });
    }
}

export default DbPostgres;


