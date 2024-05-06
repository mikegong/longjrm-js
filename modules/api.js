import Db from "../database/db.js";
import { config, dbInfos } from '../load-config.js';
import express from 'express'; // global variable express
import { dbConnectionPool } from "../startPool.js";

const DATA_FETCH_LIMIT = config.DATA_FETCH_LIMIT;

const databaseName = 'mysql-test';

const pool = await dbConnectionPool();

const apiRouter = express.Router();

// Get user list, return maxium DATA_FETCH_LIMIT rows
apiRouter.get('/read-data', async (req, res) => {
    const connection = await pool.getConnection(databaseName);
    const db = new Db(connection);

    console.log(req.body);

    const stmt = `select * from sample limit ${DATA_FETCH_LIMIT}`;
    const result = await db.query({ sql: stmt});
    await pool.releaseConnection(databaseName, connection);
    return res.json(result.data);
});

export default apiRouter;