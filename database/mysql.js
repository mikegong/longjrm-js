import Db from './db.js';

class DbMysql extends Db {
    constructor(conn) {
        super(conn);
    }

    async insert(table, data) {
        const sql = `INSERT INTO ${table} SET ?`;
        return new Promise((resolve, reject) => {
            this.conn.query(sql, data, (error, results) => {
                if (error) {
                    reject(error);
                } else {
                    const selectSql = `SELECT * FROM ${table} WHERE id = ${results.insertId}`;
                    this.conn.query(selectSql, (err, rows) => {
                        if (err) reject(err);
                        else resolve(rows);
                    });
                }
            });
        });
    }

    async update(table, data, whereCondition) {
        const whereKeys = Object.keys(whereCondition).map(key => `${key} = ?`).join(' AND ');
        const whereValues = Object.values(whereCondition);

        const updateFields = Object.keys(data).map(key => `${key} = ?`);
        const updateValues = Object.values(data);
        
        const sql = `UPDATE ${table} SET ${updateFields.join(', ')} WHERE ${whereKeys}`;

        return new Promise((resolve, reject) => {
            this.conn.query(sql, [...updateValues, ...whereValues], (error, results) => {
                if (error) {
                    reject(error);
                } else if (results.affectedRows > 0) {
                    // Re-fetch the updated rows
                    const selectSql = `SELECT * FROM ${table} WHERE ${whereKeys}`;
                    this.conn.query(selectSql, whereValues, (err, rows) => {
                        if (err) reject(err);
                        else resolve(rows);
                    });
                } else {
                    resolve([]);
                }
            });
        });
    }

    async delete(table, whereCondition) {
        const whereKeys = Object.keys(whereCondition).map(key => `${key} = ?`).join(' AND ');
        const whereValues = Object.values(whereCondition);

        const selectSql = `SELECT * FROM ${table} WHERE ${whereKeys}`;
        return new Promise((resolve, reject) => {
            this.conn.query(selectSql, whereValues, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    // Now delete those rows
                    const deleteSql = `DELETE FROM ${table} WHERE ${whereKeys}`;
                    this.conn.query(deleteSql, whereValues, (error, results) => {
                        if (error) reject(error);
                        else resolve(rows);  // Return the originally fetched rows that were deleted
                    });
                }
            });
        });
    }

    async selectAll(table) {
        const sql = `SELECT * FROM ${table}`;
        return new Promise((resolve, reject) => {
            this.conn.query(sql, (error, results) => {
                if (error) reject(error);
                else resolve(results);
            });
        });
    }
}

export default DbMysql;
