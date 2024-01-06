import { createPool } from 'generic-pool';
import DatabaseConnection from './connection.js';
import logger from '../logger.js';
import { config, dbInfos } from '../load-config.js';

class DatabaseConnectionPool {
  constructor() {
    // connection pools for various databases
    this.pools = {};
  }

  // It's stange that neither autostart true nor pool.start() can start the pool (connection).
  // The pool can only be started by calling pool.acquire().
  // Noted by MG on 2024-01-01

  async initializePool(databaseName) {
    const dbInfo = dbInfos[databaseName];
    const databaseType = dbInfo.type;

    const opts = {
      min: config.MIN_CONN_POOL_SIZE,
      max: config.MAX_CONN_POOL_SIZE,
      acquireTimeoutMillis: config.ACQUIRE_TIMEOUT,
      destroyTimeoutMillis: config.DESTROY_TIMEOUT,
      idleTimeoutMillis: config.IDLE_TIMEOUT,
      autostart: false
    }

    try {
      const factory = {
        create: async () => {
          const dbConnection = new DatabaseConnection(dbInfo);
          return await dbConnection.connect();
        },
        destroy: async (connection) => {
          await connection.close();
        }
      }

      this.pools[databaseName] = createPool(factory, opts);

    } catch (error) {
      logger.error(`Failed to initialize ${databaseType} ${dbInfo.databaseName} database connection pool: ${error.message}`);
      throw error;
    }
  }

  async startPool(databaseName) {
    try {
      await this.pools[databaseName].start();
      console.log("pool started");
      logger.info(`Started ${databaseName} database connection pool`);
    } catch (error) {
      console.log("pool started failed");
      logger.error(`Failed to start ${databaseName} database connection pool: ${error.message}`);
      throw error;
    }
  }

  async getConnection(databaseName) {
    try {
      const connection = await this.pools[databaseName].acquire();
      logger.info(`Acquired ${databaseName} database connection`);
      return connection;
    } catch (error) {
      logger.error(`Failed to acquire ${databaseName} database connection: ${error.message}`);
      throw error;
    }
  }

  async releaseConnection(databaseName, connection) {
    try {
      await this.pools[databaseName].release(connection);
      logger.info(`Released ${databaseName} database connection`);
    } catch (error) {
      logger.error(`Failed to release ${databaseName} database connection: ${error.message}`);
      throw error;
    }
  }

  async closeConnection(databaseName, connection) {
    try {
      await this.pools[databaseName].destroy(connection);
      logger.info(`Closed ${databaseName} database connection`);
    } catch (error) {
      logger.error(`Failed to close ${databaseName} database connection: ${error.message}`);
      throw error;
    }
  }

  async getPoolList() {
    return Object.keys(this.pools);
  }

  async getPoolSize(databaseName) {
    return this.pools[databaseName].size;
  }

  async getPoolAvailable(databaseName) {
    return this.pools[databaseName].available;
  }

  async getPoolPending(databaseName) {
    return this.pools[databaseName].pending;
  }

  async drainDatabasePool(databaseName) {
    logger.info(`Draining ${databaseName} database connection pool`);
    try {
      await this.pools[databaseName].drain();
      await this.pools[databaseName].clear();
      logger.info(`Shutdown ${databaseName} database connection pool`);
    } catch (error) {
      logger.error(`Failed to shutdown ${databaseName} database connection pool: ${error.message}`);
      throw error;
    }
  }
}

export default DatabaseConnectionPool;
