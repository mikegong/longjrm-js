import mysql from 'mysql2';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import logger from '../logger.js';

class DatabaseConnection {
    constructor(dbinfo) {
        this.databaseType = dbinfo.type;
        this.host = dbinfo.host;
        this.databaseName = dbinfo.database;
        this.user = dbinfo.user;
        this.password = dbinfo.password;
        this.port = dbinfo.port;
        this.client = null;
    }

    async connect() {
        const connectionErrorMsg = `Failed to connect to the ${this.databaseType} database ${this.databaseName} at ${this.host}:${this.port}`
        const connectionMsg = `Connected to the ${this.databaseType} database ${this.databaseName} at ${this.host}:${this.port}`
        try {
            const dbUrl = `${this.databaseType}://${this.user}:${this.password}@${this.host}:${this.port}/${this.databaseName}`;
            switch (this.databaseType) {
                case 'mysql':
                    this.client = mysql.createConnection(dbUrl);
                    await new Promise((resolve, reject) => {
                        this.client.connect(err => {
                            if (err) {
                                logger.error(`${connectionErrorMsg}, error number ${err.errno}, SQL state ${err.sqlState}`)
                                reject(new Error(err.message));
                            } else {
                                logger.info(`${connectionMsg}, connection thread: ${this.client.threadId}`)
                                resolve();
                            }
                        });
                    });
                    break;
                case 'postgres':
                case 'postgresql':
                    this.client = new pg.Client(dbUrl);
                    await this.client.connect();
                    logger.info(`${connectionMsg}, _connected: ${this.client._connected}, connection process ID: ${this.client.processID}`)
                    break;
                case 'mongodb':
                    this.client = new MongoClient(dbUrl, { useNewUrlParser: true });
                    await this.client.connect();
                    break;
                default:
                    return new Promise((resolve, reject) => {
                        reject(new Error(`Unsupported database type: ${this.databaseType}`));
                    });
            }
            this.client.databaseType = this.databaseType;
            return this.client;
        } catch (error) {
            logger.error(`Connection Error: ${error.message}`);
            throw error;
        }
    }

    async close() {
        try {
            const closeMsg = `Closed ${this.databaseType} ${this.databaseName} database connection at ${this.host}:${this.port}`
            if (this.client) {
                switch (this.databaseType) {
                    case 'mysql':
                        await this.client.close();
                        logger.info(`${closeMsg}, connection thread: ${this.client.threadId}`)
                        break;
                    case 'postgres':
                    case 'postgresql':
                        await this.client.end();
                        logger.info(`${closeMsg}, _connected: ${this.client._connected}, connection process ID: ${this.client.processID}`)
                        break;
                    case 'mongodb':
                        await this.client.close();
                        break;
                    default:
                        throw new Error(`Unsupported database type: ${this.databaseType}`);                    
                }
                this.client = null;
                logger.info(`Disconnected from the ${this.databaseType} database ${this.databaseName}`);
            }
        } catch (error) {
            logger.error(`Failed to disconnect from the ${this.databaseType} database ${this.databaseName}: ${error.message}`);
            throw error;
        }
    }

}

export default DatabaseConnection;
