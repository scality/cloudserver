const assert = require('assert');
const { errors, ArsenalError } = require('arsenal');

const { rateLimitDefaultConfigCacheTTL, rateLimitDefaultBurstCapacity } = require('../../../../constants');
const { calculateInterval } = require('./gcra');

/**
 * Parse and validate the complete rate limiting configuration
 *
 * @param {Object} rateLimitingConfig - config.rateLimiting object from config.json
 * @param {number} clusters - Number of worker clusters (must be numeric)
 * @returns {Object} Fully parsed and validated rate limiting configuration
 * @throws {Error} If configuration is invalid
 */
function parseRateLimitConfig(rateLimitingConfig, clusters) {
    const parsed = {
        enabled: true,
    };

    // Validate and set serviceUserArn
    assert.strictEqual(
        typeof rateLimitingConfig.serviceUserArn, 'string',
        'rateLimiting.serviceUserArn must be a string'
    );
    parsed.serviceUserArn = rateLimitingConfig.serviceUserArn;

    // Parse and validate node count
    if (rateLimitingConfig.nodes !== undefined) {
        assert(
            typeof rateLimitingConfig.nodes === 'number' &&
            Number.isInteger(rateLimitingConfig.nodes) &&
            rateLimitingConfig.nodes > 0,
            'rateLimiting.nodes must be a positive integer'
        );
        parsed.nodes = rateLimitingConfig.nodes;
    } else {
        parsed.nodes = 1; // Default to 1 node
    }

    // Parse bucket configuration
    // Always initialize bucket config to support per-bucket rate limits set via API
    // This ensures caching and default values work even when no global default is configured
    parsed.bucket = {
        defaultConfig: undefined,  // No global default unless specified
        configCacheTTL: rateLimitDefaultConfigCacheTTL,  // Default cache TTL
        defaultBurstCapacity: rateLimitDefaultBurstCapacity,  // Default burst capacity for per-bucket configs
    };

    // Override with user-provided bucket configuration
    if (rateLimitingConfig.bucket) {
        assert.strictEqual(
            typeof rateLimitingConfig.bucket, 'object',
            'rateLimiting.bucket must be an object'
        );

        // Parse default config for buckets (global default applied to all buckets)
        // If defaultConfig is specified: Parse and validate the bucket rate limit settings
        if (rateLimitingConfig.bucket.defaultConfig) {
            const bucketConfig = rateLimitingConfig.bucket.defaultConfig;

            // Validate config structure
            assert.strictEqual(
                typeof bucketConfig, 'object',
                'rate limit config must be an object'
            );

            const limitConfig = {};

            if (bucketConfig.requestsPerSecond) {
                assert.strictEqual(
                    typeof bucketConfig.requestsPerSecond, 'object',
                    'requestsPerSecond must be an object'
                );

                const { limit } = bucketConfig.requestsPerSecond;

                // Validate limit
                assert(
                    typeof limit === 'number' && Number.isInteger(limit) && limit >= 0,
                    'requestsPerSecond.limit must be a non-negative integer'
                );

                // Validate limit against nodes AND workers
                const minLimit = parsed.nodes * clusters;
                if (limit > 0 && limit < minLimit) {
                    throw new Error(
                        `requestsPerSecond.limit (${limit}) must be >= ` +
                        `(nodes × workers = ${parsed.nodes} × ${clusters} = ${minLimit}) ` +
                        'or 0 (unlimited). Each worker enforces limit/nodes/workers locally. ' +
                        `With limit < ${minLimit}, per-worker rate would be < 1 req/s, effectively blocking traffic.`
                    );
                }

                // Default to global default burst capacity
                let burstCapacity = parsed.bucket.defaultBurstCapacity;

                // Override if provided in config
                if (bucketConfig.requestsPerSecond.burstCapacity !== undefined) {
                    burstCapacity = bucketConfig.requestsPerSecond.burstCapacity;
                    assert(
                        typeof burstCapacity === 'number' && Number.isInteger(burstCapacity) && burstCapacity > 0,
                        'requestsPerSecond.burstCapacity must be a positive integer'
                    );
                }

                // Calculate per-worker interval using distributed architecture
                const interval = calculateInterval(limit, parsed.nodes, clusters);

                limitConfig.requestsPerSecond = {
                    interval,
                    bucketSize: burstCapacity * 1000,
                };
            }

            parsed.bucket.defaultConfig = limitConfig;
        }

        // Parse config cache TTL
        // If configCacheTTL is specified: Override the default cache TTL
        if (rateLimitingConfig.bucket.configCacheTTL !== undefined) {
            const configCacheTTL = rateLimitingConfig.bucket.configCacheTTL;
            assert(
                typeof configCacheTTL === 'number' &&
                Number.isInteger(configCacheTTL) &&
                configCacheTTL > 0,
                'rateLimiting.bucket.configCacheTTL must be a positive integer'
            );
            parsed.bucket.configCacheTTL = configCacheTTL;
        }
    }

    // Parse error configuration (supports any HTTP 4xx/5xx status code)
    // Default to SlowDown error
    parsed.error = errors.SlowDown;

    // Override with custom error if specified
    if (rateLimitingConfig.error !== undefined) {
        // Validate error is an object
        assert.strictEqual(
            typeof rateLimitingConfig.error, 'object',
            'rateLimiting.error must be an object'
        );

        // If statusCode is specified, validate and create custom error
        if (rateLimitingConfig.error.statusCode !== undefined) {
            assert(
                typeof rateLimitingConfig.error.statusCode === 'number' &&
                Number.isInteger(rateLimitingConfig.error.statusCode) &&
                rateLimitingConfig.error.statusCode >= 400 &&
                rateLimitingConfig.error.statusCode < 600,
                'rateLimiting.error.statusCode must be a valid HTTP status code (400-599)'
            );

            // Validate error code if provided
            const errorCode = rateLimitingConfig.error.code || 'SlowDown';
            if (rateLimitingConfig.error.code !== undefined) {
                assert.strictEqual(
                    typeof rateLimitingConfig.error.code, 'string',
                    'rateLimiting.error.code must be a string'
                );
            }

            // Validate error message if provided
            const errorMessage = rateLimitingConfig.error.message || errors.SlowDown.description;
            if (rateLimitingConfig.error.message !== undefined) {
                assert.strictEqual(
                    typeof rateLimitingConfig.error.message, 'string',
                    'rateLimiting.error.message must be a string'
                );
            }

            // Override default with custom Arsenal error
            parsed.error = new ArsenalError(
                errorCode,
                rateLimitingConfig.error.statusCode,
                errorMessage
            );
        }
    }

    return parsed;
}

module.exports = {
    parseRateLimitConfig,
};
