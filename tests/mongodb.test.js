import 'dotenv/config';
import DatabaseConnection from '../connection/connection.js'; 
import DbMongo from '../database/mongodb.js'; 
import { dbInfos } from '../load-config.js'; 

async function main() {
    const databaseName = 'mongodb-test'; 
    const dbInfo = dbInfos[databaseName];

    const dbConnection = new DatabaseConnection(dbInfo);
    try {
        const conn = await dbConnection.connect();
        const db = new DbMongo(conn);

        // Insert Test
        const insertData = { name: 'Test User', email: 'test@example.com' };
        const inserted = await db.insert('users', insertData);
        console.log('Insert Result:', inserted);

        // Select Test - Fetching all users
        const selected = await db.selectAll('users');
        console.log('Select Result:', selected);

        // Update Test
        const updateData = { name: 'Updated User' };
        const whereUpdate = { email: 'test@example.com' };
        const updated = await db.update('users', whereUpdate, updateData);
        console.log('Update Result:', updated);

        // Delete Test
        const whereDelete = { email: 'test@example.com' };
        const deleted = await db.delete('users', whereDelete);
        console.log('Delete Result:', deleted);

    } catch (error) {
        console.error('Error during DB operations:', error);
    } finally {
        await dbConnection.close();
    }
}

main();
