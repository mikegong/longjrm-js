import mysql from 'mysql2';
import DatabaseConnection from './connection.js';
import logger from '../logger.js';

export default class MySQLConnection extends DatabaseConnection {
  async connect() {
    try {
      const port = this.port ? `:${this.port}` : '';
      const dbUrl = `${this.databaseType}://${this.user}:${this.password}@${this.host}${port}/${this.databaseName}`;
      this.client = mysql.createConnection(dbUrl);

      await new Promise((resolve, reject) => {
        this.client.connect(err => {
          if (err) {
            logger.error(`${this.getConnectionErrorMsg()}, error number ${err.errno}, SQL state ${err.sqlState}`);
            reject(new Error(err.message));
          } else {
            logger.info(`${this.getConnectionMsg()}, connection thread: ${this.client.threadId}`);
            this.setClientMetadata();  // Set the metadata
            resolve();
          }
        });
      });
      return this.client;
    } catch (error) {
      logger.error(`Connection Error: ${error.message}`);
      throw error;
    }
  }
}
