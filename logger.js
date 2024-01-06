import pino from 'pino';
import 'dotenv/config';

const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    prettyPrint: true,
    timestamp: () => `,"time":"${new Date().toISOString()}"`,
    formatters: {
        level: (label) => `,"level":"${label}"`
    }
});

export default logger;