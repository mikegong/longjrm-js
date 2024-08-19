import Db from './db.js';

const dbModuleMap = {
    'postgres': 'postgres.js',
    'postgresql': 'postgres.js',
    'mysql': 'mysql.js',
    'mongodb': 'mongodb.js',
    'mongodb+srv': 'mongodb.js',
    'odbc': 'odbc.js'
}

class DbFactory {

    constructor(conn) {
        this.conn = conn;
    }

    async createDb() {
        let dbModule;
        if (dbModuleMap[this.conn.databaseType]) {
            dbModule = await import(`./${dbModuleMap[this.conn.databaseType]}`);
        } else {
            dbModule = Db;
        }
        const DbClass = dbModule.default;
        if (this.conn.databaseType in dbModuleMap) {
            return new DbClass(this.conn);
        } else {
            return new Db(this.conn);
        }
    }
}

export default DbFactory;
