import 'dotenv/config';
import DatabaseConnection from '../connection/connection.js';
import { dbInfos } from '../load-config.js';
import Db from '../database/db.js'; // Ensure this path is correct

async function main() {
    const database = {
        'mysql': 'mysql-test',                          
        'postgres': 'postgres-test',
        'mongodb': 'mongodb-test'               
    };  
    const dbtype = 'mongodb'; // Set the DB type you want to test
    const dbInfo = dbInfos[database[dbtype]];                   

    const dbConnection = new DatabaseConnection(dbInfo);        
    try {
        const conn = await dbConnection.connect();  
        const db = new Db(conn);    

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
        const whereUpdate = [{ email: 'test@example.com' }]; 
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





