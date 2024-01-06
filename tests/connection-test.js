import 'dotenv/config';
import DatabaseConnection from '../connection/connection.js';
import mysql from 'mysql2';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import { config, dbInfos } from '../load-config.js';

const mysqlDbName = "mysql-test";
const postgresDbName = "postgres-test";
const databaseName = postgresDbName;
const dbInfo = dbInfos[databaseName];

const dbConnection = new DatabaseConnection(dbInfo);

try {
    await dbConnection.connect();
} catch (error) {
    console.error(error);
}

await dbConnection.close();