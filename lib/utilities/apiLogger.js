const { Werelogs } = require('werelogs');
const fs = require('fs');
const path = require('path');

const _config = require('../Config.js').config;

/**
 * Create API operations logger with separate Werelogs instance
 * This logger is specifically for S3 API operation logging and writes to a separate file
 */

let apiWerelogs = null;
let apiLogger = null;

function createApiLogger() {
    // Check if API logging is enabled
    if (!_config.apiLog || !_config.apiLog.enabled) {
        // Return a no-op logger if API logging is disabled
        return {
            info: () => {},
            debug: () => {},
            warn: () => {},
            error: () => {},
            trace: () => {},
            fatal: () => {},
        };
    }

    // Ensure logs directory exists
    const outputFile = _config.apiLog.outputFile || './logs/api-operations.log';
    const logDir = path.dirname(outputFile);

    try {
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir, { recursive: true });
        }
    } catch (error) {
        // Fall back to console-only logging if directory creation fails
        console.warn('Failed to create API log directory, falling back to console logging:', error.message);

        apiWerelogs = new Werelogs({
            level: _config.apiLog.logLevel || 'info',
            dump: _config.apiLog.dumpLevel || 'error',
            streams: [
                { level: 'trace', stream: process.stdout }
            ]
        });

        return new apiWerelogs.Logger('API');
    }

    // Create file stream for API logs
    const apiLogStream = fs.createWriteStream(outputFile, { flags: 'a' });

    // Handle stream errors
    apiLogStream.on('error', (error) => {
        console.error('API log file stream error:', error);
    });

    // Create the API-specific Werelogs instance - file output only
    apiWerelogs = new Werelogs({
        level: _config.apiLog.logLevel || 'info',
        dump: _config.apiLog.dumpLevel || 'error',
        streams: [{ level: 'trace', stream: apiLogStream }]
    });

    return new apiWerelogs.Logger('API');
}

// Create the logger instance
try {
    apiLogger = createApiLogger();
} catch (error) {
    console.error('Failed to create API logger, using no-op logger:', error);
    // Fallback to no-op logger
    apiLogger = {
        info: () => {},
        debug: () => {},
        warn: () => {},
        error: () => {},
        trace: () => {},
        fatal: () => {},
    };
}

module.exports = apiLogger;
