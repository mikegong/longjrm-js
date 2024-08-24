// dynamic import of config and dbInfos modules based on NODE_ENV
import 'dotenv/config';

let config = {};
let dbInfos = {};

export async function loadConfiguration(externalConfig = {}, externalDbInfos = {}, customLogger = null) {
  const environment = process.env.NODE_ENV || 'development';

  try {
    // Dynamically import configuration files based on NODE_ENV
    const configModule = await import(`./config-${environment}.js`);
    const environmentConfig = configModule.default || {};
    
    const dbInfosModule = await import(`./dbinfos-${environment}.js`);
    const environmentDbInfos = dbInfosModule.default || {};

    // Merge configurations (top-level replacement)
    config = { ...environmentConfig, ...externalConfig };
    dbInfos = { ...environmentDbInfos, ...externalDbInfos };

    // Optionally log the configuration if a custom logger is provided
    if (customLogger) {
      customLogger.info(`JRM environment initialized with configuration: ${JSON.stringify(config, null, 2)}`);
    }

    // Return the loaded configurations for further handling
    return { config, dbInfos };
    
  } catch (error) {
    // Use custom logger if provided, otherwise fallback to console
    if (customLogger) {
      customLogger.error(`Error loading configuration for environment ${environment}:`, error);
    } else {
      console.error(`Error loading configuration for environment ${environment}:`, error);
    }
    process.exit(1); // Exit the process if config loading fails
  }
}

// Optionally, the caller can still use this if they prefer automatic initialization
await loadConfiguration();

// Export the configurations
export { config, dbInfos };
