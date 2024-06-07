import Db from './db.js';

const dbModuleMap = {
    'postgres': 'postgres.js',
    'postgresql': 'postgres.js',
    'mysql': 'mysql.js',
    'mongodb': 'mongodb.js',
    'mongodb+srv': 'mongodb.js'
}

class DbFactory {

    constructor(conn) {
        this.conn = conn;
    }

    async createDb() {
        const dbModule = await import(`./${dbModuleMap[this.conn.databaseType]}`);
        const DbClass = dbModule.default;
        if (this.conn.databaseType in dbModuleMap) {
            return new DbClass(this.conn);
        } else {
            return new Db(this.conn);
        }
    }
}

export default DbFactory;
