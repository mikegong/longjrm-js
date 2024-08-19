import MySQLConnection from './mysql-conn.js';
import PostgreSQLConnection from './postgres-conn.js';
import MongoDBConnection from './mongo-conn.js';
import ODBCConnection from './odbc-conn.js';

function createDatabaseConnection(dbinfo) {
    switch (dbinfo.type) {
      case 'mysql':
        return new MySQLConnection(dbinfo);
      case 'postgres':
      case 'postgresql':
        return new PostgreSQLConnection(dbinfo);
      case 'mongodb':
      case 'mongodb+srv':
        return new MongoDBConnection(dbinfo);
      case 'odbc':
        return new ODBCConnection(dbinfo);
      default:
        throw new Error(`Unsupported database type: ${dbinfo.type}`);
    }
  }
  
  export { createDatabaseConnection };
  