import 'dotenv/config';
import DatabaseConnection from '../connection/connection.js';
import PostgresDb from '../database/postgres.js';
import { dbInfos } from '../load-config.js';

async function main() {
    const databaseName = 'postgres-test'; // Use the appropriate config name for PostgreSQL
    const dbInfo = dbInfos[databaseName];

    const dbConnection = new DatabaseConnection(dbInfo);
    try {
        const conn = await dbConnection.connect();
        const db = new PostgresDb(conn);

        // Insert Test
        const insertData = { name: 'Test User', email: 'test@example.com' };
        const inserted = await db.insert({ table: 'users', data: insertData });
        console.log('Insert Result:', inserted);

        // Select Test
        const whereSelect = [{ email: 'test@example.com' }];
        const selected = await db.select({ table: 'users', where: whereSelect });
        console.log('Select Result:', selected);

        // Update Test
        const updateData = { name: 'Updated User' };
        const whereUpdate = [{ email: 'test102@example.com' }];
        const updated = await db.update({ table: 'users', data: updateData, where: whereUpdate });
        console.log('Update Result:', updated);

        // Delete Test
        const whereDelete = [{ email: 'test@example.com' }];
        const deleted = await db.delete({ table: 'users', where: whereDelete });
        console.log('Delete Result:', deleted);

    } catch (error) {
        console.error('Error during DB operations:', error);
    } finally {
        await dbConnection.close();
    }
}

main();

