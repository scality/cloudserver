const assert = require('assert');
const { errors } = require('arsenal');

const constants = require('../../../../../constants');
const { parseRateLimitConfig } = require('../../../../../lib/api/apiUtils/rateLimit/config');

describe('parseRateLimitConfig', () => {
    const validConfig = {
        serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
        nodes: 2,
        bucket: {
            defaultConfig: {
                requestsPerSecond: {
                    limit: 1000,
                    burstCapacity: 10,
                },
            },
            configCacheTTL: 300,
        },
        error: {
            statusCode: 503,
            code: 'ServiceUnavailable',
            message: 'Service Unavailable',
        },
    };

    describe('valid configurations', () => {
        it('should parse complete valid configuration', () => {
            const result = parseRateLimitConfig(validConfig, 10);

            assert.strictEqual(result.enabled, true);
            assert.strictEqual(result.serviceUserArn, validConfig.serviceUserArn);
            assert.strictEqual(result.nodes, 2);
            assert.strictEqual(result.bucket.configCacheTTL, 300);
            assert.strictEqual(result.error.code, 503);
            assert.strictEqual(result.error.message, 'ServiceUnavailable');
            assert.strictEqual(result.error.description, 'Service Unavailable');
            assert(result.bucket.defaultConfig);
            assert(result.bucket.defaultConfig.requestsPerSecond);
        });

        it('should use default values when optional fields are omitted', () => {
            const minimalConfig = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(minimalConfig, 5);

            assert.strictEqual(result.enabled, true);
            assert.strictEqual(result.serviceUserArn, minimalConfig.serviceUserArn);
            assert.strictEqual(result.nodes, 1); // Default
            // Bucket config is always initialized for per-bucket rate limiting via API
            assert(result.bucket);
            assert.strictEqual(result.bucket.defaultConfig, undefined); // No global default
            assert.strictEqual(result.bucket.configCacheTTL, constants.rateLimitDefaultConfigCacheTTL); // Default
            assert.strictEqual(result.bucket.defaultBurstCapacity, constants.rateLimitDefaultBurstCapacity); // Default
            assert.strictEqual(result.error.code, errors.SlowDown.code); // Default
            assert.strictEqual(result.error.description, errors.SlowDown.description); // Default
        });

        it('should parse configuration with bucket but no defaultConfig', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    configCacheTTL: 600,
                },
            };

            const result = parseRateLimitConfig(config, 3);

            assert.strictEqual(result.bucket.configCacheTTL, 600);
            assert.strictEqual(result.bucket.defaultConfig, undefined);
        });

        it('should use default configCacheTTL when not specified', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {},
            };

            const result = parseRateLimitConfig(config, 2);

            assert.strictEqual(result.bucket.configCacheTTL, constants.rateLimitDefaultConfigCacheTTL);
        });

        it('should default to SlowDown error when error object has no code', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {},
            };

            const result = parseRateLimitConfig(config, 1);

            assert.strictEqual(result.error.code, errors.SlowDown.code);
            assert.strictEqual(result.error.description, errors.SlowDown.description);
        });
    });

    describe('serviceUserArn validation', () => {
        it('should throw if serviceUserArn is missing', () => {
            const config = { nodes: 1 };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.serviceUserArn must be a string/
            );
        });

        it('should throw if serviceUserArn is not a string', () => {
            const config = {
                serviceUserArn: 12345,
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.serviceUserArn must be a string/
            );
        });
    });

    describe('nodes validation', () => {
        it('should accept valid positive integer for nodes', () => {
            const config = {
                ...validConfig,
                nodes: 5,
            };

            const result = parseRateLimitConfig(config, 10);
            assert.strictEqual(result.nodes, 5);
        });

        it('should default nodes to 1 when not specified', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(config, 2);
            assert.strictEqual(result.nodes, 1);
        });

        it('should throw if nodes is negative', () => {
            const config = {
                ...validConfig,
                nodes: -1,
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.nodes must be a positive integer/
            );
        });

        it('should throw if nodes is zero', () => {
            const config = {
                ...validConfig,
                nodes: 0,
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.nodes must be a positive integer/
            );
        });

        it('should throw if nodes is not an integer', () => {
            const config = {
                ...validConfig,
                nodes: 2.5,
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.nodes must be a positive integer/
            );
        });

        it('should throw if nodes is not a number', () => {
            const config = {
                ...validConfig,
                nodes: 'two',
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.nodes must be a positive integer/
            );
        });
    });

    describe('bucket validation', () => {
        it('should throw if bucket is not an object', () => {
            const config = {
                ...validConfig,
                bucket: 'invalid',
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.bucket must be an object/
            );
        });

        it('should parse bucket with valid defaultConfig', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 500,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config, 5);

            assert(result.bucket.defaultConfig);
            assert(result.bucket.defaultConfig.requestsPerSecond);
        });

        it('should throw if defaultConfig is not an object', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: 'invalid',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rate limit config must be an object/
            );
        });

        it('should throw if requestsPerSecond is not an object', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: 'invalid',
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /requestsPerSecond must be an object/
            );
        });

        it('should accept limit = 0 (unlimited)', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 0,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config, 1);
            // limit = 0 means unlimited, should be accepted
            assert(result.bucket.defaultConfig.requestsPerSecond);
        });

        it('should propagate validation errors for negative limit', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: -100, // Invalid
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /requestsPerSecond.limit must be a non-negative integer/
            );
        });
    });

    describe('burstCapacity validation', () => {
        it('should use default burstCapacity when not provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config, 1);
            const bucketSize = result.bucket.defaultConfig.requestsPerSecond.bucketSize;
            // bucketSize = burstCapacity * 1000
            assert.strictEqual(bucketSize, constants.rateLimitDefaultBurstCapacity * 1000);
        });

        it('should use custom burstCapacity when provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 20,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config, 1);
            const bucketSize = result.bucket.defaultConfig.requestsPerSecond.bucketSize;
            assert.strictEqual(bucketSize, 20 * 1000);
        });

        it('should throw if burstCapacity is negative', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: -5,
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /requestsPerSecond.burstCapacity must be a positive integer/
            );
        });

        it('should throw if burstCapacity is zero', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 0,
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /requestsPerSecond.burstCapacity must be a positive integer/
            );
        });

        it('should throw if burstCapacity is not an integer', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 5.5,
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /requestsPerSecond.burstCapacity must be a positive integer/
            );
        });

        it('should throw if burstCapacity is not a number', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 'ten',
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /requestsPerSecond.burstCapacity must be a positive integer/
            );
        });
    });

    describe('bucket.configCacheTTL validation', () => {
        it('should accept valid positive integer for configCacheTTL', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    configCacheTTL: 450,
                },
            };

            const result = parseRateLimitConfig(config, 1);
            assert.strictEqual(result.bucket.configCacheTTL, 450);
        });

        it('should throw if configCacheTTL is negative', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    configCacheTTL: -100,
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.bucket.configCacheTTL must be a positive integer/
            );
        });

        it('should throw if configCacheTTL is zero', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    configCacheTTL: 0,
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.bucket.configCacheTTL must be a positive integer/
            );
        });

        it('should throw if configCacheTTL is not an integer', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    configCacheTTL: 100.5,
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.bucket.configCacheTTL must be a positive integer/
            );
        });

        it('should throw if configCacheTTL is not a number', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    configCacheTTL: 'three hundred',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.bucket.configCacheTTL must be a positive integer/
            );
        });
    });

    describe('error configuration validation', () => {
        it('should throw if error is not an object', () => {
            const config = {
                ...validConfig,
                error: 'invalid',
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.error must be an object/
            );
        });

        it('should accept valid HTTP 4xx and 5xx error codes', () => {
            // Test 4xx error code
            const config4xx = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 429,
                    message: 'Too Many Requests',
                },
            };

            const result4xx = parseRateLimitConfig(config4xx, 1);
            assert.strictEqual(result4xx.error.code, 429);
            assert.strictEqual(result4xx.error.description, 'Too Many Requests');

            // Test 5xx error code
            const config5xx = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 503,
                    message: 'Service Unavailable',
                },
            };

            const result5xx = parseRateLimitConfig(config5xx, 1);
            assert.strictEqual(result5xx.error.code, 503);
            assert.strictEqual(result5xx.error.description, 'Service Unavailable');
        });

        it('should throw if error code is less than 400', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 399,
                    message: 'Invalid',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.error.statusCode must be a valid HTTP status code \(400-599\)/
            );
        });

        it('should throw if error code is 600 or greater', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 600,
                    message: 'Invalid',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.error.statusCode must be a valid HTTP status code \(400-599\)/
            );
        });

        it('should throw if error code is not an integer', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 503.5,
                    message: 'Invalid',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.error.statusCode must be a valid HTTP status code \(400-599\)/
            );
        });

        it('should throw if error code is not a number', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: '503',
                    message: 'Invalid',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.error.statusCode must be a valid HTTP status code \(400-599\)/
            );
        });

        it('should use default message when message is not provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 503,
                },
            };

            const result = parseRateLimitConfig(config, 1);
            assert.strictEqual(result.error.code, 503);
            assert.strictEqual(result.error.message, 'SlowDown');
            assert.strictEqual(result.error.description, errors.SlowDown.description);
        });

        it('should throw if error message is not a string', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 503,
                    message: 123,
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.error.message must be a string/
            );
        });

        it('should use SlowDown error when no error config is provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(config, 1);
            assert.strictEqual(result.error.code, errors.SlowDown.code);
            assert.strictEqual(result.error.description, errors.SlowDown.description);
        });

        it('should use custom error type when code is provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 429,
                    code: 'TooManyRequests',
                    message: 'Please slow down',
                },
            };

            const result = parseRateLimitConfig(config, 1);
            assert.strictEqual(result.error.code, 429);
            assert.strictEqual(result.error.message, 'TooManyRequests');
            assert.strictEqual(result.error.description, 'Please slow down');
        });

        it('should default error type to SlowDown when code is not provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 429,
                    message: 'Please slow down',
                },
            };

            const result = parseRateLimitConfig(config, 1);
            assert.strictEqual(result.error.code, 429);
            assert.strictEqual(result.error.message, 'SlowDown');
            assert.strictEqual(result.error.description, 'Please slow down');
        });

        it('should throw if error code is not a string', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 503,
                    code: 123,
                    message: 'Invalid',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 1),
                /rateLimiting.error.code must be a string/
            );
        });
    });

    describe('distributed rate limiting validation', () => {
        it('should validate limit against nodes and workers', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                nodes: 5,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 10, // Less than 5 nodes × 10 workers = 50
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config, 10),
                /requestsPerSecond\.limit \(10\) must be >= \(nodes × workers = 5 × 10 = 50\)/
            );
        });
    });

    describe('calculation verification', () => {
        it('should calculate correct interval for distributed setup', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                nodes: 2,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100, // 100 req/s global
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config, 5); // 5 workers

            // Per-worker rate = 100 / 2 nodes / 5 workers = 10 req/s
            // Interval = 1000ms / 10 = 100ms
            const interval = result.bucket.defaultConfig.requestsPerSecond.interval;
            assert.strictEqual(interval, 100);
        });

        it('should calculate correct bucketSize from burstCapacity', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 15,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config, 1);

            // bucketSize = burstCapacity * 1000
            const bucketSize = result.bucket.defaultConfig.requestsPerSecond.bucketSize;
            assert.strictEqual(bucketSize, 15 * 1000);
        });

        it('should handle single node and single worker', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                nodes: 1,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 50, // 50 req/s
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config, 1); // 1 worker

            // Per-worker rate = 50 / 1 / 1 = 50 req/s
            // Interval = 1000ms / 50 = 20ms
            const interval = result.bucket.defaultConfig.requestsPerSecond.interval;
            assert.strictEqual(interval, 20);
        });

        it('should handle high-scale distributed setup', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                nodes: 10,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 10000, // 10,000 req/s global
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config, 20); // 20 workers per node

            // Per-worker rate = 10000 / 10 nodes / 20 workers = 50 req/s
            // Interval = 1000ms / 50 = 20ms
            const interval = result.bucket.defaultConfig.requestsPerSecond.interval;
            assert.strictEqual(interval, 20);
        });
    });
});
