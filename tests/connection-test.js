import 'dotenv/config';
import DatabaseConnection from '../connection/connection.js';
import mysql from 'mysql2';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import { config, dbInfos } from '../load-config.js';

const database = {
    'mysql': 'mysql-test',
    'postgres': 'postgres-test',
    'mongodb': 'mongodb-test'
};

const dbtype = 'postgres';

const dbInfo = dbInfos[database[dbtype]];

const dbConnection = new DatabaseConnection(dbInfo);

try {
    const conn = await dbConnection.connect();
    if (conn.databaseType = 'mongodb+srv') {
        const database = conn.db('test')
        const listings = database.collection('Listing');
        const query = { title: 'Nice Cottage' };
        const cottage = await listings.findOne(query);
        console.log(cottage);
    }
} catch (error) {
    console.error(error);
}

await dbConnection.close();