import { MongoClient } from 'mongodb';
import DatabaseConnection from './connection.js';
import logger from '../logger.js';

export default class MongoDBConnection extends DatabaseConnection {
  async connect() {
    try {
      const dbUrl = `${this.databaseType}://${this.user}:${this.password}@${this.host}/${this.databaseName}`;
      this.client = new MongoClient(dbUrl);
      await this.client.connect();
      logger.info(`${this.getConnectionMsg()}`);
      this.setClientMetadata();  // Set the metadata
      return this.client;
    } catch (error) {
      logger.error(`Connection Error: ${error.message}`);
      throw error;
    }
  }
}

