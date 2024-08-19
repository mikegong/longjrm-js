import logger from '../logger.js';

class DatabaseConnection {
    constructor(dbinfo) {
        this.databaseType = dbinfo.type;
        this.host = dbinfo.host;
        this.databaseName = dbinfo.database;
        this.user = dbinfo.user;
        this.password = dbinfo.password;
        this.port = dbinfo.port;
        this.dsn = dbinfo?.dsn;
        this.autocommit = dbinfo['autocommit'] || true;
        this.client = null;
    }

    async connect() {
        throw new Error('connect() must be implemented by subclasses');
    }

    async close() {
        try {
          if (this.client) {
            await this.client.close();
            logger.info(`Closed ${this.databaseType} database connection at ${this.host}:${this.port}`);
            this.client = null;
          }
        } catch (error) {
          logger.error(`Failed to disconnect from the ${this.databaseType} database ${this.databaseName}: ${error.message}`);
          throw error;
        }
      }

    getConnectionErrorMsg() {
        return `Failed to connect to the ${this.databaseType} database '${this.databaseName}' at ${this.host}:${this.port}`;
    }

    getConnectionMsg() {
        return `Connected to the ${this.databaseType} database '${this.databaseName}' at ${this.host}:${this.port}`;
    }

    getCloseMsg() {
        return `Closed ${this.databaseType} database connection at ${this.host}:${this.port}`;
    }

    // Shared method to set metadata
    setClientMetadata() {
        if (this.client) {
            this.client.databaseType = this.databaseType;
            this.client.databaseName = this.databaseName;
        }
    }
}

export default DatabaseConnection;
