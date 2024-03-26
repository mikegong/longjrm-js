// dynamic import of config and dbinfos modules based on NODE_ENV

import 'dotenv/config';
import logger from './logger.js';

let config;
let dbInfos;

try {
  const configModule = await import(`./env/config-${process.env.NODE_ENV}.js`);
  config = configModule.default;

  const dbInfosModule = await import(`./env/dbinfos-${process.env.NODE_ENV}.js`);
  dbInfos = dbInfosModule.default;
  logger.info('JRM environment initialized');
} catch (error) {
  logger.error('Error loading the module:', error);
};

export { config, dbInfos };
