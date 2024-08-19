import pg from 'pg';
import DatabaseConnection from './connection.js';

export default class PostgreSQLConnection extends DatabaseConnection {
  async connect() {
    try {
      const port = this.port ? `:${this.port}` : '';
      const dbUrl = `${this.databaseType}://${this.user}:${this.password}@${this.host}${port}/${this.databaseName}`;
      this.client = new pg.Client(dbUrl);
      await this.client.connect();
      logger.info(`${this.getConnectionMsg()}, _connected: ${this.client._connected}, connection process ID: ${this.client.processID}`);
      this.setClientMetadata();  // Set the metadata
      return this.client;
    } catch (error) {
      logger.error(`Connection Error: ${error.message}`);
      throw error;
    }
  }

  async close() {
    try {
      if (this.client) {
        await this.client.end();
        logger.info(`${this.getCloseMsg()}, _connected: ${this.client._connected}, connection process ID: ${this.client.processID}`);
        this.client = null;
      }
    } catch (error) {
      logger.error(`Failed to disconnect from the PostgreSQL database: ${error.message}`);
      throw error;
    }
  }
}
