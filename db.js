/*
Description: sql library that wrap crud sql statements via db2 ibm_db api.
conn - database connection
table - the table that is populated
data - json data including columns and values
condition - json data that defines where columns and values
*/

const config = require('./config');
const connection = require('./connection');
const fs = require('fs');
const DATA_FETCH_LIMIT = config.DATA_FETCH_LIMIT;
const ibmdb = require('ibm_db');

// convert sql where condition in JSON object to string
function conditionToString(condition) {

        var where_str = "";
        // console.log(condition);
        if (typeof (condition) !== "undefined" && Object.keys(condition).length !== 0) {
                Object.keys(condition).forEach(function (k) {
                        if (typeof (condition[k]) == "string") {
                                const escapedValue = condition[k].replace(/'/g, "''"); // escape single quote
                                where_str += " AND " + k + "='" + escapedValue + "'";
                        }
                        else
                                where_str += " AND " + k + "=" + condition[k];
                })
                where_str = " where " + where_str.substring(5);
        }
        // console.log(where_str);
        return where_str;
}


// execute query synchronously via db2 ibm_db api
function executeQuerySync(sql) {
        let conn = null, stmt = null, result = null;

        try {
                conn = connection.getDb2ConnSync();
        } catch (e) {
                return { "status": -1, "message": e.message, "error": e };
        }

        try {
                stmt = conn.prepareSync(sql);
                result = stmt.executeSync();
                const data = result.fetchAllSync();
                let total = 0;
                if (typeof (data) === "object")
                        total = Object.keys(data).length;

                result.closeSync();
                stmt.closeSync();
                connection.returnDb2ConnSync(conn);
                return { "status": 0, "message": "SQL statement succeeded", "data": data, "total": total };
        } catch (e) {
                console.error(e.message);
                if (typeof result !== "undefined") result.closeSync();
                if (typeof stmt !== "undefined") stmt.closeSync();
                if (e.sqlcode == null || e.sqlcode != -30081)
                        connection.returnDb2ConnSync(conn);
                else
                        connection.closeDb2ConnSync(conn);
                return { "status": -1, "message": e.message, "error": e };
        }
}


// execute query with mode synchronously via db2 ibm_db api
// TODO: replace executeQuerySync
function executeQuerySyncWithMode({ sql, mode = "select" }) {
        let conn = null;

        try {
                conn = connection.getDb2ConnSync();
        } catch (e) {
                return { "status": -1, "message": e.message, "error": e };
        }

        var data = []
        try {
                var stmt = conn.prepareSync(sql);
                var result = stmt.executeSync();
                if (mode === "select")
                        data = result.fetchAllSync();
                result.closeSync();
                stmt.closeSync();
                connection.returnDb2ConnSync(conn);
                return { "status": 0, "message": "SQL statement succeeded", "data": data };
        } catch (e) {
                console.error(e.message);
                if (typeof result !== "undefined") result.closeSync();
                if (typeof stmt !== "undefined") stmt.closeSync();
                if (e.sqlcode == null || e.sqlcode != -30081)
                        connection.returnDb2ConnSync(conn);
                else
                        connection.closeDb2ConnSync(conn);
                return { "status": -1, "message": e.message, "error": e };
        }
}

// execute insert array query synchronously via db2 ibm_db api
function executeArrayInsertQuerySyncWithMode({ sql, value, size, mode = "select" }) {

        let conn = null;

       try {
               conn = connection.getDb2ConnSync();
       } catch (e) {
               return { "status": -1, "message": e.message, "error": e };
       }
         
       var data = []
       try {
               var stmt = conn.prepareSync(sql)
               stmt.bindSync(value);
               stmt.setAttrSync(ibmdb.SQL_ATTR_PARAMSET_SIZE, size)
               var result = stmt.executeSync();
               if (mode === "select")
                       data = result.fetchAllSync();
               result.closeSync();
               stmt.closeSync();
               connection.returnDb2ConnSync(conn);
               return { "status": 0, "message": "SQL statement succeeded", "data": data };
       } catch (e) {
               console.error(e.message);
               if (typeof result !== "undefined") result.closeSync();
               if (typeof stmt !== "undefined") stmt.closeSync();
               if (e.sqlcode == null || e.sqlcode != -30081)
                       connection.returnDb2ConnSync(conn);
               else
                       connection.closeDb2ConnSync(conn);
               return { "status": -1, "message": e.message, "error": e };
       }
}


// execute non query such as insert/update/delete asynchronously
async function executeNonQuery({sql, values=[], callback}) {
        let conn = null;

        try {
                conn = await connection.getDb2Conn();
        } catch(err) {
                return callback({ "status": -1, "message": err.message, "error": err})
        }

        conn.prepare(sql, function (err, stmt) {

                if (err) {
                        //could not prepare for some reason
                        console.error(err);
                        if (typeof stmt !== "undefined") stmt.closeSync();
                        if (err.sqlcode == null || err.sqlcode != -30081)
                                connection.returnDb2Conn(conn);
                        else
                                connection.closeDb2Conn(conn);
                        return callback({ "status": -1, "message": err.message, "error": err });
                }

                stmt.executeNonQuery(function (err, ret) {
                        stmt.closeSync();
                        if (err) {
                                console.error(err);
                                if (err.sqlcode == null || err.sqlcode != -30081)
                                        connection.returnDb2Conn(conn);
                                else
                                        connection.closeDb2Conn(conn);
                                return callback({ "status": -1, "message": err.message, "error": err });
                        }
                        else {
                                let total = 0;
                                if (ret != -1) total = ret;
                                connection.returnDb2Conn(conn);
                                return callback({ "status": 0, "message": `SQL statement succeeded. ${total} rows is affected.`, "data": [], "total": total });
                        }
                });
        });
}


// execute prepared statement asynchronously
async function executeQuery({sql, values=[], callback}) {
        let result = {};
        let conn = null;

        try {
                conn = await connection.getDb2Conn();
        } catch(err) {
                return callback({ "status": -1, "message": err.message, "error": err})
        }

        conn.prepare(sql, function (err, stmt) {
                if (err) {
                        //could not prepare for some reason
                        console.error(err);
                        if (typeof stmt !== "undefined") stmt.closeSync();
                        if (err.sqlcode == null || err.sqlcode != -30081)
                                connection.returnDb2Conn(conn);
                        else
                                connection.closeDb2Conn(conn);
                        return callback({ "status": -1, "message": err.message, "error": err });
                }

                stmt.execute(values, function (err, result) {
                        if (err) {
                                if (typeof result !== "undefined") result.closeSync();
                                stmt.closeSync();
                                if (err.sqlcode == null || err.sqlcode != -30081)
                                        connection.returnDb2Conn(conn);
                                else
                                        connection.closeDb2Conn(conn);
                                console.log(err);
                                return callback({ "status": -1, "message": err.message, "error": err });
                        }
                        else {
                                result.fetchAll({ fetchMode: 4 }, function (err, data, colcount) {
                                        result.closeSync();
                                        stmt.closeSync();
                                        // console.log(data);
                                        if (err) {
                                                console.error(err);
                                                if (err.sqlcode == null || err.sqlcode != -30081)
                                                        connection.returnDb2Conn(conn);
                                                else
                                                        connection.closeDb2Conn(conn);
                                                return callback({ "status": -1, "message": err.message, "error": err });
                                        }
                                        else {
                                                let total = 0;
                                                if (typeof (data) === "object")
                                                        total = Object.keys(data).length;

                                                connection.returnDb2Conn(conn);
                                                return callback({ "status": 0, "message": `SQL query succeeded. ${total} rows is returned.`, "data": data, "total": total });
                                        }
                                });
                        }
                });
        });
}

// select from a single table synchronously
function selectSync({ table, columns = ["*"], condition, limit = DATA_FETCH_LIMIT }) {

        let sql = '';

        if (Array.isArray(columns))
                var column_string = columns.join();
        else
                return { "status": -1, "message": "invalid column" };

        const where_str = conditionToString(condition);

        if (limit == 0)
                sql = "select " + column_string + " from " + table + where_str;
        else
                sql = "select " + column_string + " from " + table + where_str + " fetch first " + limit + " rows only";

        return executeQuerySync(sql);
}


// select from a single table asynchronously
function select({ table, columns = ["*"], condition, values=[], limit = DATA_FETCH_LIMIT, orderBy = '', callback }) {

        let sql = '';
        let column_string = ''

        if (Array.isArray(columns))
                column_string = columns.join();
        else
                return callback({ "status": -1, "message": "invalid column" });

        const where_str = conditionToString(condition);

        const order_str = orderBy ? ' order by ' + orderBy + ' ' : '';

        if (limit == 0)
                sql = "select " + column_string + " from " + table + where_str + order_str;
        else
                sql = "select " + column_string + " from " + table + where_str + order_str + " fetch first " + limit + " rows only";
        // console.log(sql);
        executeQuery({sql: sql, values: values, callback: (result) => { callback(result) }});
}


// run a sql query synchronously
function querySync({ sql, limit = DATA_FETCH_LIMIT }) {
        let limit_str = "";

        if (sql.indexOf("fetch first") === -1) {
                if (limit != 0)
                        limit_str = " fetch first " + limit + " rows only";
                sql = sql + limit_str;
        }
        // console.log(sql);
        return executeQuerySyncWithMode({ sql: sql });
}


// run a sql query asynchronously
function query({ sql, values=[], limit = DATA_FETCH_LIMIT, callback }) {
        let limit_str = "";

        if (sql.indexOf("fetch first") === -1) {
                if (limit != 0)
                        limit_str = " fetch first " + limit + " rows only";
                sql = sql + limit_str;
        }
        executeQuery({sql: sql, values: values, callback: (result) => { callback(result) } });
}


// insert data with json format (pairs of column and value) into database table
function insert({table, data, values=[], callback}) {

        var strCol = ''
        var strVal = ''
        var sql = ''
        var dataValue = ''

        Object.keys(data).forEach(function (k) {
                strCol += ',' + k
                if (typeof (data[k]) == "object") {
                        if (data[k] === null)
                                dataValue = null;
                        else
                                dataValue = JSON.stringify(data[k]);
                }
                else {
                        dataValue = data[k]
                }
                if (typeof (dataValue) == "string") {
                        const escapedValue = dataValue.replace(/'/g, "''"); // escape single quote
                        strVal += ",'" + escapedValue + "'"
                }
                else {
                        strVal += "," + dataValue
                }
        })

        sql = `INSERT INTO ${table} (${strCol.substring(1)}) VALUES (${strVal.substring(1)})`;
        // console.log(sql);
        executeNonQuery({sql: sql, values: values, callback: (result) => { callback(result) }});
}

// a generic insert function and return inserted column
function insertReturn({ table, data, values=[], return_columns = [], callback }) {

        var strCol = ''
        var strVal = ''
        var sql = ''
        var dataValue = ''

        Object.keys(data).forEach(function (k) {
                strCol += ',' + k
                if (typeof (data[k]) == "object"){
                        if (data[k] === null)
                                dataValue = null;
                        else
                                dataValue = JSON.stringify(data[k]);
                }
                else {
                        dataValue = data[k]
                }
                
                if (typeof (dataValue) == "string") {
                        const escapedValue = dataValue.replace(/'/g, "''"); // escape single quote
                        // console.log({escapedValue});
                        strVal += ",'" + escapedValue + "'"
                }
                else {
                        strVal += "," + dataValue
                }
        })

        if (return_columns.length === 0) {
                sql = `INSERT INTO ${table} (${strCol.substring(1)}) VALUES (${strVal.substring(1)})`;
                executeNonQuery({sql: sql, values: values, callback: (result) => { callback(result) }});
        }
        else {
                sql = `SELECT ${return_columns.join()} FROM FINAL TABLE (INSERT INTO ${table} (${strCol.substring(1)}) VALUES (${strVal.substring(1)}))`;
                // console.log(sql);
                executeQuery({sql: sql, values: values, callback: (result) => { callback(result) }});
        }
}

// sychroize insert data with json format (pairs of column and value) into database table
function insertSync(table, data) {

        var strCol = ''
        var strVal = ''
        var sql = ''
        var dataValue = ''

        Object.keys(data).forEach(function (k) {
                strCol += ',' + k
                if (typeof (data[k]) == "object")
                        dataValue = JSON.stringify(data[k])
                else {
                        dataValue = data[k]
                }
                if (typeof (dataValue) == "string") {
                        const escapedValue = dataValue.replace(/'/g, "''"); // escape single quote                       
                        strVal += ",'" + escapedValue + "'"
                }
                else {
                        strVal += "," + dataValue
                }
        })

        sql = `INSERT INTO ${table} (${strCol.substring(1)}) VALUES (${strVal.substring(1)})`;

        return executeQuerySyncWithMode({ sql: sql, mode: "insert" });
}

// sychroize insert data with json object into database table
function insertSyncWO({table, data}) {
        // console.log({table, data})
         var strCol = ''
         var strVal = ''
         var sql = ''
         var dataValue = ''
 
         Object.keys(data).forEach(function (k) {
                 strCol += ',' + k
                 if (typeof (data[k]) == "object")
                         dataValue = JSON.stringify(data[k])
                 else {
                         dataValue = data[k]
                 }
                 if (typeof (dataValue) == "string") {
                         const escapedValue = dataValue.replace(/'/g, "''"); // escape single quote
                         strVal += ",'" + escapedValue + "'"
                 }
                 else {
                         strVal += "," + dataValue
                 }
         })
 
         sql = `INSERT INTO ${table} (${strCol.substring(1)}) VALUES (${strVal.substring(1)})`;
 
         return executeQuerySyncWithMode({ sql: sql, mode: "insert" });
 }

  // sychroize insert multiple rows of data into database table
function insertArraySync(table, data, key_columns, size) {
        var strCol = ''
        var strVal = ''
        var sql = ''
        var dataValue = data
        
        Object.values(key_columns).forEach( k => {
                strCol += ',' + k
                strVal += ',' + '?'
        })
        
        sql = `INSERT INTO ${table} (${strCol.substring(1)}) VALUES (${strVal.substring(1)})`;
        return executeArrayInsertQuerySyncWithMode({ sql: sql, value: dataValue, size: size, mode: "insert"});
}


function update({ table, update_data, condition, values=[], callback }) {

        if (typeof (condition) == "undefined" || Object.keys(condition).length == 0) {
                return callback({ "status": 1, "message": "Warning: Update condition is empty" })
        }

        var update_str = '';
        var where_str = conditionToString(condition);

        Object.keys(update_data).forEach(function (k) {
                if (typeof (update_data[k]) == "string") {
                        // update_data[k].replace("'", "''"); // escape single quote
                        const escapedValue = update_data[k].replace(/'/g, "''"); // escape single quote
                        if (update_data[k] != "$CURRENT_TIMESTAMP")
                                update_str += "," + k + "='" + escapedValue + "'";
                        else
                                update_str += "," + k + "= CURRENT_TIMESTAMP";
                }
                else
                        update_str += "," + k + "=" + update_data[k];
        })

        var sql = "UPDATE " + table + " SET " + update_str.substring(1) + where_str;
        // console.log(sql);
        executeNonQuery({sql: sql, values: values, callback: (result) => { callback(result) }});
}

async function bulkUpdate({ table, bulk_data, callback }) {
        // bulk_data: a list of update data object
        let conn = null;

        try {
                conn = await connection.getDb2Conn();
        } catch(err) {
                return callback({ "status": -1, "message": err.message, "error": err})
        }

        conn.beginTransaction(function (err) {
                if (err) {
                        // could not begin a transaction for some reason
                        console.log(err);
                        if (err.sqlcode == null || err.sqlcode != -30081)
                                connection.returnDb2Conn(conn);
                        else
                                connection.closeDb2Conn(conn);
                        return callback({ "status": -1, "message": err.message, "error": err });
                }

                bulk_data.forEach(function (data) {
                        const update_result = updateSync({ table: table, update_data: data.update_data, condition: data.condition });
                        if (update_result.status != 0)
                                return callback(update_result);
                })

                conn.commitTransaction(function (err) {
                        if (err) {
                                console.log(err);
                                if (err.sqlcode == null || err.sqlcode != -30081)
                                        connection.returnDb2Conn(conn);
                                else
                                        connection.closeDb2Conn(conn);
                                return callback({ "status": -1, "message": err.message, "error": err, "data": [] });
                        }
                        else {
                                connection.returnDb2Conn(conn);
                                return callback({ "status": 0, "message": "Bulk update succeeded", "data": [] });
                        }
                })
        })
}

async function bulkInsert({ table, bulk_data, callback }) {
        let conn = null;

        try {
                conn = await connection.getDb2Conn();
        } catch(err) {
                return callback({ "status": -1, "message": err.message, "error": err})
        }

        conn.beginTransaction(function (err) {
                if (err) {
                        // could not begin a transaction for some reason
                        console.log(err);
                        if (err.sqlcode == null || err.sqlcode != -30081)
                                connection.returnDb2Conn(conn);
                        else
                                connection.closeDb2Conn(conn);
                        return callback({ "status": -1, "message": err.message, "error": err });
                }

                bulk_data.forEach(function (data_update) {
                        // console.log(data_update, table)
                        const update_result = insertSyncWO({ table: table, data: data_update });
                        if (update_result.status != 0)
                                return callback(update_result);
                })

                conn.commitTransaction(function (err) {
                        if (err) {
                                console.log(err);
                                if (err.sqlcode == null || err.sqlcode != -30081)
                                        connection.returnDb2Conn(conn);
                                else
                                        connection.closeDb2Conn(conn);
                                return callback({ "status": -1, "message": err.message, "error": err, "data": [] });
                        }
                        else {
                                connection.returnDb2Conn(conn);
                                return callback({ "status": 0, "message": "Bulk update succeeded", "data": [] });
                        }
                })
        })
}

async function bulkMerge({ table, bulk_data, key_columns, callback }) {
        let conn = null;

        try {
                conn = await connection.getDb2Conn();
        } catch(err) {
                return callback({ "status": -1, "message": err.message, "error": err})
        }

        conn.beginTransaction(function (err) {
                if (err) {
                        // could not begin a transaction for some reason
                        console.log(err);
                        if (err.sqlcode == null || err.sqlcode != -30081)
                                connection.returnDb2Conn(conn);
                        else
                                connection.closeDb2Conn(conn);
                        return callback({ "status": -1, "message": err.message, "error": err });
                }

                bulk_data.forEach(function (data_update) {
                        const update_result = mergeSync({ table: table, data: data_update, key_columns: key_columns});
                        if (update_result.status != 0)
                                return callback(update_result);
                })

                conn.commitTransaction(function (err) {
                        if (err) {
                                console.log(err);
                                if (err.sqlcode == null || err.sqlcode != -30081)
                                        connection.returnDb2Conn(conn);
                                else
                                        connection.closeDb2Conn(conn);
                                return callback({ "status": -1, "message": err.message, "error": err, "data": [] });
                        }
                        else {
                                connection.returnDb2Conn(conn);
                                return callback({ "status": 0, "message": "Bulk update succeeded", "data": [] });
                        }
                })
        })
}


function updateSync({ table, update_data, condition }) {

        if (typeof (condition) == "undefined" || Object.keys(condition).length == 0) {
                return { "status": 1, "message": "Warning: Update condition is empty" }
        }

        var update_str = '';
        var where_str = conditionToString(condition);

        Object.keys(update_data).forEach(function (k) {
                if (typeof (update_data[k]) == "string") {
                        const escapedValue = update_data[k].replace(/'/g, "''"); // escape single quote
                        if (update_data[k] != "$CURRENT_TIMESTAMP")
                                update_str += "," + k + "='" + escapedValue + "'";
                        else
                                update_str += "," + k + "= CURRENT_TIMESTAMP";
                }
                else
                        update_str += "," + k + "=" + update_data[k];
        })

        var sql = "UPDATE " + table + " SET " + update_str.substring(1) + where_str;

        return executeQuerySyncWithMode({ sql: sql, mode: "update" });
}

function sql_delete({ table, condition, values=[], callback }) {

        if (typeof (condition) == "undefined" || Object.keys(condition).length == 0) {
                return callback({ "status": 1, "message": "Warning: Delete condition is empty" });
        }

        var where_str = '';

        if (typeof (condition) !== "undefined" && Object.keys(condition).length !== 0) {
                Object.keys(condition).forEach(function (k) {
                        if (typeof (condition[k]) == "string") {
                                const escapedValue = condition[k].replace(/'/g, "''"); // escape single quote
                                where_str += " AND " + k + "='" + escapedValue + "'";
                        }
                        else
                                where_str += " AND " + k + "=" + condition[k];
                })
                where_str = " where " + where_str.substring(5)
        }

        var sql = "DELETE FROM " + table + where_str;
        // console.log(sql);
        executeNonQuery({sql: sql, values: values, callback: (result) => { callback(result) }});
}


function sql_deleteSync({ table, condition }) {

        if (typeof (condition) == "undefined" || Object.keys(condition).length == 0) {
                return ({ "status": 1, "message": "Warning: Delete condition is empty" });
        }

        var where_str = '';

        if (typeof (condition) !== "undefined" && Object.keys(condition).length !== 0) {
                Object.keys(condition).forEach(function (k) {
                        if (typeof (condition[k]) == "string") {
                                const escapedValue = condition[k].replace(/'/g, "''"); // escape single quote
                                where_str += " AND " + k + "='" + escapedValue + "'";
                        }
                        else
                                where_str += " AND " + k + "=" + condition[k];
                })
                where_str = " where " + where_str.substring(5)
        }

        var sql = "DELETE FROM " + table + where_str;

        return executeQuerySyncWithMode({ sql: sql, mode: "delete" });
}


function merge({ table, data, key_columns, values=[], callback }) {
        var column_str = Object.keys(data).toString();
        var match_str = '';
        var insert_str = '';
        var update_str = '';
        var value_str = '';

        key_columns.forEach(function (v) {
                match_str += " AND A." + v + " = TMP." + v;
        })

        Object.keys(data).forEach(function (v) {
                insert_str += ", TMP." + v;
        })

        Object.keys(data).forEach(function (k) {
                if (typeof (data[k]) == "string") {
                        const escapedValue = data[k].replace(/'/g, "''"); // escape single quote
                        update_str += "," + k + "='" + escapedValue + "'";
                        value_str += ", '" + escapedValue + "'";
                }
                else {
                        update_str += "," + k + "=" + data[k];
                        value_str += "," + data[k]
                }
        })

        match_str = match_str.substring(5);
        insert_str = insert_str.substring(1);
        update_str = update_str.substring(1);
        value_str = value_str.substring(1);


        var sql = "MERGE INTO " + table + " AS A USING (VALUES ( " + value_str
                + ")) AS TMP (" + column_str + ") ON " + match_str
                + " WHEN MATCHED THEN UPDATE SET " + update_str
                + " WHEN NOT MATCHED THEN INSERT (" + column_str + ") VALUES (" + insert_str
                + ") ELSE IGNORE";
        executeNonQuery({sql: sql, values: values, callback: (result) => { callback(result) }});
}

function mergeSync({ table, data, key_columns }) {
        var column_str = Object.keys(data).toString();
        var match_str = '';
        var insert_str = '';
        var update_str = '';
        var value_str = '';

        key_columns.forEach(function (v) {
                match_str += " AND A." + v + " = TMP." + v;
        })

        Object.keys(data).forEach(function (v) {
                insert_str += ", TMP." + v;
        })

        Object.keys(data).forEach(function (k) {
                if (typeof (data[k]) == "string") {
                        const escapedValue = data[k].replace(/'/g, "''"); // escape single quote
                        update_str += "," + k + "='" + escapedValue + "'";
                        value_str += ", '" + escapedValue + "'";
                }
                else {
                        update_str += "," + k + "=" + data[k];
                        value_str += "," + data[k]
                }
        })

        match_str = match_str.substring(5);
        insert_str = insert_str.substring(1);
        update_str = update_str.substring(1);
        value_str = value_str.substring(1);


        var sql = "MERGE INTO " + table + " AS A USING (VALUES ( " + value_str
                + ")) AS TMP (" + column_str + ") ON " + match_str
                + " WHEN MATCHED THEN UPDATE SET " + update_str
                + " WHEN NOT MATCHED THEN INSERT (" + column_str + ") VALUES (" + insert_str
                + ") ELSE IGNORE";
        return executeQuerySyncWithMode({ sql: sql, mode: "merge" });
}

// read sql from file to run async
function runSqlFromFile({ filename, where = "", callback, limit = DATA_FETCH_LIMIT }) {
        fs.readFile(filename, 'utf8', function (err, data) {
                if (err) {
                        console.log(err);
                        return callback({ "status": -1, "message": err.message, "error": err });
                }
                else {
                        if (where !== "")
                                data = data + " " + where;
                        query({
                                sql: data, limit: limit, callback: (result) => {
                                        callback(result);
                                }
                        });
                }
        });
}


// read sql from file to run sync
function runSqlFromFileSync({ filename, where = "", limit = DATA_FETCH_LIMIT }) {
        let data;
        try {
                data = fs.readFileSync(filename, { encoding: 'utf8', flag: 'r' });
        }
        catch (e) {
                console.log(e.message);
                return { "status": -1, "message": e.message };
        }

        if (where !== "")
                data = data + " " + where;
        var result = querySync({ sql: data, limit: limit });
        return result;
}


exports.select = select;
exports.query = query;
exports.executeNonQuery = executeNonQuery;
exports.selectSync = selectSync;
exports.querySync = querySync;
exports.insert = insert;
exports.insertSync = insertSync;
exports.insertReturn = insertReturn;
exports.update = update;
exports.bulkUpdate = bulkUpdate;
exports.bulkInsert = bulkInsert;
exports.updateSync = updateSync;
exports.insertSyncWO = insertSyncWO;
exports.delete = sql_delete;
exports.deleteSync = sql_deleteSync;
exports.merge = merge;
exports.mergeSync = mergeSync;
exports.conditionToString = conditionToString;
exports.runSqlFromFile = runSqlFromFile;
exports.runSqlFromFileSync = runSqlFromFileSync;
exports.insertArraySync = insertArraySync;
exports.bulkMerge = bulkMerge;

