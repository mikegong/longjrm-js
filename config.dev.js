const config = {
  'API_PORT': 8000,
  'HTTPS_PORT': 8001,
  'WEB_PORT': 3000,
  'DB_TIMEOUT': 40, // database connection timeout
  'DATA_FETCH_LIMIT': 1000, // default data fetch limit. 0 means unlimit
  'MIN_CONN_POOL_SIZE': 2,
  'MAX_CONN_POOL_SIZE': 50,
  'ACQUIRE_TIMEOUT': 10000,
  'DESTROY_TIMEOUT': 5000,
}

export default config;
