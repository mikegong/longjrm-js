import pino from 'pino';
import 'dotenv/config';

const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    timestamp: () => `,"time":"${new Date().toISOString()}"`,
    transport: {
        targets: [
            {
                target: 'pino-pretty',
                options: { translateTime: 'yyyy-mm-dd HH:MM:ss.l' }
            },
            {
                target: 'pino-pretty',
                options: {
                    destination: process.env.LOG_FILE,
                    translateTime: 'yyyy-mm-dd HH:MM:ss.l',
                    colorize: false
                }
            }
        ],
    },
});

export default logger;