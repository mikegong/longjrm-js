import 'dotenv/config';
import { createDatabaseConnection } from '../connection/conn-factory.js';
import { config, dbInfos } from '../env/load-config.js';

const database = {
    'mysql': 'mysql-test',
    'postgres': 'postgres-test',
    'mongodb': 'mongodb-test',
    'odbc': 'odbc-test'
};

const dbtype = 'odbc';

const dbInfo = dbInfos[database[dbtype]];

const dbConnection = new createDatabaseConnection(dbInfo);

try {
    console.log('Connecting to the database...');
    const conn = await dbConnection.connect();
    if (conn.databaseType == 'mongodb+srv') {
        const database = conn.db('test')
        const listings = database.collection('Listing');
        const query = { title: 'Nice Cottage' };
        const cottage = await listings.findOne(query);
        console.log(cottage);
    }
    else {
        const result = await conn.query('SELECT * FROM sample');
        result.forEach(row => {
            console.log('Row:', row);
      
            // Access specific columns by name
            console.log('Column1:', row.c1);
            console.log('Column2:', row.c2);
            // Add more columns as needed
          });
    }
} catch (error) {
    console.error(error);
}

await dbConnection.close();