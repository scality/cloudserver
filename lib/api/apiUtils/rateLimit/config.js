const Joi = require('@hapi/joi');
const { errors, ArsenalError } = require('arsenal');

const { rateLimitDefaultConfigCacheTTL, rateLimitDefaultBurstCapacity } = require('../../../../constants');

/**
 * Full Rate Limiting Configuration Example
 *
 * Add this to your config.json to enable rate limiting:
 *
 * {
 *   "rateLimiting": {
 *     // Required: Enable/disable rate limiting
 *     "enabled": true,
 *
 *     // Required: Service user ARN (bypasses rate limits)
 *     "serviceUserArn": "arn:aws:iam::123456789012:root",
 *
 *     // Optional: Number of CloudServer nodes in distributed deployment
 *     // Default: 1
 *     // Used to calculate per-worker rate limit: limit / nodes / workers
 *     "nodes": 1,
 *
 *     // Optional: Bucket-level rate limiting configuration
 *     "bucket": {
 *       // Optional: Global default rate limit for all buckets
 *       // Limit can be overridden per-bucket via PutBucketRateLimit API
 *       "defaultConfig": {
 *         "requestsPerSecond": {
 *           // Required: Requests per second limit (0 = unlimited)
 *           "limit": 100,
 *
 *           // Optional: Burst capacity (allows temporary spikes)
 *           // Default: 1 (no burst allowed)
 *           // Higher values allow more requests in quick succession
 *           "burstCapacity": 2
 *         }
 *       },
 *
 *       // Optional: How long to cache rate limit configs (milliseconds)
 *       // Default: 30000 (30 seconds)
 *       // Higher values = better performance, slower config updates
 *       "configCacheTTL": 30000,
 *
 *       // Optional: Default burst capacity for per-bucket configs
 *       // Default: 1
 *       "defaultBurstCapacity": 1
 *     },
 *
 *     // Optional: Account-level rate limiting configuration
 *     "account": {
 *       // Optional: Global default rate limit for all accounts
 *       // Limit can be overridden per-account via UpdateAccountLimits API
 *       "defaultConfig": {
 *         "requestsPerSecond": {
 *           // Required: Requests per second limit (0 = unlimited)
 *           "limit": 100,
 *
 *           // Optional: Burst capacity (allows temporary spikes)
 *           // Default: 1 (no burst allowed)
 *           // Higher values allow more requests in quick succession
 *           "burstCapacity": 2
 *         }
 *       },
 *
 *       // Optional: How long to cache rate limit configs (milliseconds)
 *       // Default: 30000 (30 seconds)
 *       // Higher values = better performance, slower config updates
 *       "configCacheTTL": 30000,
 *
 *       // Optional: Default burst capacity for per-account configs
 *       // Default: 1
 *       "defaultBurstCapacity": 1
 *     },
 *
 *     // Optional: Custom error response for rate limited requests
 *     "error": {
 *       // Optional: HTTP status code (400-599)
 *       // Default: 503 (SlowDown)
 *       "statusCode": 429,
 *
 *       // Optional: Error code
 *       // Default: "SlowDown"
 *       "code": "TooManyRequests",
 *
 *       // Optional: Error message
 *       // Default: "Please reduce your request rate."
 *       "message": "Please reduce your request rate"
 *     }
 *
 *      // Optional tuning parameters
 *      "tokenBucketBufferSize": 50,
 *      "tokenBucketRefillThreshold": 20,
 *
 *     // NOTE: Token reservation refills happen automatically every 100ms.
 *     // No additional configuration needed for worker coordination.
 *   }
 * }
 *
 * Minimal Configuration (uses defaults):
 * {
 *   "rateLimiting": {
 *     "enabled": true,
 *     "serviceUserArn": "arn:aws:iam::123456789012:root"
 *   }
 * }
 *
 * Complete Configuration (all options):
 * {
 *   "rateLimiting": {
 *     "enabled": true,
 *     "serviceUserArn": "arn:aws:iam::123456789012:root",
 *     "nodes": 3,
 *     "bucket": {
 *       "defaultConfig": {
 *         "requestsPerSecond": {
 *           "limit": 1000,
 *           "burstCapacity": 5
 *         }
 *       },
 *       "configCacheTTL": 60000,
 *     },
 *     "error": {
 *       "statusCode": 429,
 *       "code": "TooManyRequests",
 *       "message": "Rate limit exceeded. Please try again later."
 *     }
 *   }
 * }
 *
 * Priority Order:
 * 1. Per-bucket config (set via PutBucketRateLimit API) - Highest priority
 * 2. Global default config (bucket.defaultConfig) - Fallback
 * 3. No rate limiting (null) - If neither is configured
 *
 * Token Reservation Architecture:
 * - Workers request tokens in advance from Redis (not per-request)
 * - Tokens are consumed locally (in-memory, fast)
 * - Background job refills tokens every 100ms (async, non-blocking)
 * - Redis enforces node-level quota using GCRA at token grant time
 * - Busy workers automatically get more tokens (dynamic work-stealing)
 * - Total limit divided across nodes: limit / nodes = per-node quota
 * - Example: 1000 req/s with 10 nodes = 100 req/s per node
 * - Workers on same node share the node quota dynamically
 *
 * Burst Capacity:
 * - Allows temporary spikes above the sustained rate
 * - Value of 1 = 1 second of burst (can send 1s worth of requests immediately)
 * - Value of 2 = 2 seconds of burst (can send 2s worth of requests immediately)
 * - Enforced atomically in Redis during token grants using GCRA
 */

/**
 * RateLimitClassConfig
 * @typedef {Object} RateLimitClassConfig
 * @property {object} defaultConfig - Default config applied if no resource specific configuration is found.
 * @property {number} configCacheTTL - Number of milliseconds to cache per resource configs
 * @property {number} defaultBurstCapacity - Default used if resource does not specify a burst capacity.
*/

const rateLimitClassConfigSchema = Joi.object({
    defaultConfig: Joi.object({
        requestsPerSecond: Joi.object({
            limit: Joi.number().integer().min(0).required(),
            burstCapacity: Joi.number().min(0),
        }),
    }),
    configCacheTTL: Joi.number().integer().positive().default(rateLimitDefaultConfigCacheTTL),
    defaultBurstCapacity: Joi.number().min(0).default(rateLimitDefaultBurstCapacity),
}).default({
    defaultConfig: undefined,
    configCacheTTL: rateLimitDefaultConfigCacheTTL,
    defaultBurstCapacity: rateLimitDefaultBurstCapacity,
});

const rateLimitConfigSchema = Joi.object({
    enabled: Joi.boolean().default(false),
    serviceUserArn: Joi.string().required(),
    nodes: Joi.number().integer().positive().default(1),
    tokenBucketBufferSize: Joi.number().integer().positive().default(50),
    tokenBucketRefillThreshold: Joi.number().integer().positive().default(20),
    bucket: rateLimitClassConfigSchema,
    account: rateLimitClassConfigSchema,
    error: Joi.object({
        statusCode: Joi.number().integer().min(400).max(599).default(503),
        code: Joi.string().default(errors.SlowDown.message),
        message: Joi.string().default(errors.SlowDown.description),
    }).default({
        statusCode: 503,
        code: errors.SlowDown.message,
        message: errors.SlowDown.description,
    }),
}).default({
    enabled: false,
    nodes: 1,
    tokenBucketBufferSize: 50,
    tokenBucketRefillThreshold: 20,
    bucket: {
        configCacheTTL: rateLimitDefaultConfigCacheTTL,
        defaultBurstCapacity: rateLimitDefaultBurstCapacity,
    },
    account: {
        configCacheTTL: rateLimitDefaultConfigCacheTTL,
        defaultBurstCapacity: rateLimitDefaultBurstCapacity,
    },
    error: {
        statusCode: 503,
        code: errors.SlowDown.message,
        message: errors.SlowDown.description,
    },
});

/**
 * Transform validated class config (bucket or account) by calculating intervals
 * and applying business logic
 *
 * @param {string} resourceClass - Rate limit class name ('bucket' or 'account')
 * @param {object} validatedCfg - Already validated config from Joi
 * @param {number} nodes - Number of instances that requests will be load balanced across
 * @returns {RateLimitClassConfig} Transformed rate limit config
 */
function transformClassConfig(resourceClass, validatedCfg, nodes) {
    const defaultConfig = {
        RequestsPerSecond: {
            BurstCapacity: validatedCfg.defaultBurstCapacity,
        },
    };

    if (validatedCfg.defaultConfig?.requestsPerSecond) {
        const { limit, burstCapacity } = validatedCfg.defaultConfig.requestsPerSecond;

        // Validate limit against nodes (business rule)
        if (limit > 0 && limit < nodes) {
            throw new Error(
                `rateLimiting.${resourceClass}.defaultConfig.` +
                `requestsPerSecond.limit (${limit}) must be >= ` +
                `nodes (${nodes}) ` +
                'or 0 (unlimited). Each node enforces limit/nodes locally. ' +
                `With limit < ${nodes}, per-node rate would be < 1 req/s, effectively blocking traffic.`
            );
        }

        // Store both the original limit and the calculated values
        defaultConfig.RequestsPerSecond = {
            Limit: limit,
            BurstCapacity: burstCapacity ?? validatedCfg.defaultBurstCapacity,
        };
    }

    return {
        defaultConfig,
        configCacheTTL: validatedCfg.configCacheTTL,
        defaultBurstCapacity: validatedCfg.defaultBurstCapacity,
    };
}

/**
 * Parse and validate the complete rate limiting configuration
 *
 * @param {Object} rateLimitingConfig - config.rateLimiting object from config.json
 * @returns {Object} Fully parsed and validated rate limiting configuration
 * @throws {Error} If configuration is invalid
 */
function parseRateLimitConfig(rateLimitingConfig) {
    // Validate configuration using Joi schema
    const { error: validationError, value: validated } = rateLimitConfigSchema.validate(
        rateLimitingConfig,
        {
            abortEarly: false,   // Return all validation errors at once
            allowUnknown: false, // Don't allow key not present in schema
            convert: false,      // Don't do type coercion (e.g. "1" -> 1)
        }
    );

    if (validationError) {
        const details = validationError.details.map(d => d.message).join('; ');
        throw new Error(`rateLimiting configuration is invalid: ${details}`);
    }

    // Initialize parsed config with validated values (including defaults)
    const parsed = {
        enabled: validated.enabled,
        serviceUserArn: validated.serviceUserArn,
        nodes: validated.nodes,
        tokenBucketBufferSize: validated.tokenBucketBufferSize,
        tokenBucketRefillThreshold: validated.tokenBucketRefillThreshold,
        error: new ArsenalError(
            validated.error.code,
            validated.error.statusCode,
            validated.error.message,
        ),
    };

    parsed.bucket = transformClassConfig('bucket', validated.bucket, parsed.nodes);
    parsed.account = transformClassConfig('account', validated.account, parsed.nodes);

    return parsed;
}

module.exports = {
    parseRateLimitConfig,
};
