import odbc from 'odbc';

// can only use await keyword in an async function
async function getColumns() {
    // returns information about all columns in table MY_SCEHMA.MY_TABLE
    const connection = await odbc.connect('dsn=mysql-test');
    const result = await connection.columns(null, 'test', 'sample', null);
    const columns = []
    result.forEach(row => {
        columns.push(row);
    }); 
    console.log('Columns:', columns);
}

getColumns();