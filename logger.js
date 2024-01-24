import pino from 'pino';
import pretty from 'pino-pretty';
import 'dotenv/config';

const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    timestamp: () => `,"time":"${new Date().toISOString()}"`,
    formatters: {
        level: (label) => `,"level":"${label}"`
    }
});

// pretty(process.stdout);

export default logger;