import odbc from 'odbc';
import DatabaseConnection from './connection.js';
import logger from '../logger.js';

export default class ODBCConnection extends DatabaseConnection {
  async connect() {
    try {
      this.client = await odbc.connect(`dsn=${this.dsn}`);
      logger.info(`${this.getConnectionMsg()}`);
      this.setClientMetadata();  // Set the metadata
      return this.client;
    } catch (error) {
      logger.error(`Connection Error: ${error.message}`);
      throw error;
    }
  }

  getConnectionErrorMsg() {
    return `Failed to connect to the ${this.databaseType} database DSN: '${this.dsn}'`;
}

  getConnectionMsg() {
      return `Connected to the ${this.databaseType} database DSN: '${this.dsn}'`;
  }

}
