const config = {
  'DB_TIMEOUT': 40, // database connection timeout
  'DATA_FETCH_LIMIT': 1000, // default data fetch limit. 0 means unlimit
  'MIN_CONN_POOL_SIZE': 2,
  'MAX_CONN_POOL_SIZE': 50,
  'ACQUIRE_TIMEOUT': 10000,
  'DESTROY_TIMEOUT': 5000,
  'API_PORT': 3001,
  'HTTPS_PORT': 3000,
}

export default config;
