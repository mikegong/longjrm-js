// dynamic import of config and dbinfos modules based on NODE_ENV

import 'dotenv/config';
import logger from './logger.js';

let config;
let dbInfos;

try {
  const configModule = await import(`./config.${process.env.NODE_ENV}.js`);
  config = configModule.default;

  const dbInfosModule = await import(`./dbinfos.${process.env.NODE_ENV}.js`);
  dbInfos = dbInfosModule.default;
} catch (error) {
  logger.error('Error loading the module:', error);
};

export { config, dbInfos };
