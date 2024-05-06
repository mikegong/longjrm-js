import DatabaseConnectionPool from "./connection/pool.js";

export const dbConnectionPool = async () => {
  const dbConnectionPool = new DatabaseConnectionPool();
  const databaseName = 'mysql-test';

  try {
    await dbConnectionPool.initializePool(databaseName);
    await dbConnectionPool.startPool(databaseName);
    console.log('Pool initialized');
  } catch (error) {
    console.log(error.message);
    throw error;
  }

  return dbConnectionPool;
}; 
