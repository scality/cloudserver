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
            const result = parseRateLimitConfig(validConfig);

            assert.strictEqual(result.enabled, false); // Default when not specified
            assert.strictEqual(result.serviceUserArn, validConfig.serviceUserArn);
            assert.strictEqual(result.nodes, 2);
            assert.strictEqual(result.bucket.configCacheTTL, 300);
            assert.strictEqual(result.error.code, 503);
            assert.strictEqual(result.error.message, 'ServiceUnavailable');
            assert.strictEqual(result.error.description, 'Service Unavailable');
            assert(result.bucket.defaultConfig);
            assert(result.bucket.defaultConfig.RequestsPerSecond);
        });

        it('should use default values when optional fields are omitted', () => {
            const minimalConfig = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(minimalConfig);

            assert.strictEqual(result.enabled, false); // Default
            assert.strictEqual(result.serviceUserArn, minimalConfig.serviceUserArn);
            assert.strictEqual(result.nodes, 1); // Default
            assert.strictEqual(result.tokenBucketBufferSize, 50); // Default
            assert.strictEqual(result.tokenBucketRefillThreshold, 20); // Default
            // Bucket config is always initialized for per-bucket rate limiting via API
            assert(result.bucket);
            assert.deepStrictEqual(result.bucket.defaultConfig, {
                RequestsPerSecond: { BurstCapacity: constants.rateLimitDefaultBurstCapacity },
            });
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

            const result = parseRateLimitConfig(config);

            assert.strictEqual(result.bucket.configCacheTTL, 600);
            assert.deepStrictEqual(result.bucket.defaultConfig, {
                RequestsPerSecond: { BurstCapacity: constants.rateLimitDefaultBurstCapacity },
            });
        });

        it('should use default configCacheTTL when not specified', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {},
            };

            const result = parseRateLimitConfig(config);

            assert.strictEqual(result.bucket.configCacheTTL, constants.rateLimitDefaultConfigCacheTTL);
        });

        it('should default to SlowDown error when error object has no code', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {},
            };

            const result = parseRateLimitConfig(config);

            assert.strictEqual(result.error.code, errors.SlowDown.code);
            assert.strictEqual(result.error.description, errors.SlowDown.description);
        });
    });

    describe('enabled field validation', () => {
        it('should default enabled to false when not specified', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.enabled, false);
        });

        it('should accept enabled: true', () => {
            const config = {
                enabled: true,
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.enabled, true);
        });

        it('should accept enabled: false', () => {
            const config = {
                enabled: false,
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.enabled, false);
        });

        it('should throw if enabled is not a boolean', () => {
            const config = {
                enabled: 'yes',
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid/
            );
        });
    });

    describe('serviceUserArn validation', () => {
        it('should throw if serviceUserArn is missing', () => {
            const config = { nodes: 1 };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"serviceUserArn" is required/
            );
        });

        it('should throw if serviceUserArn is not a string', () => {
            const config = {
                serviceUserArn: 12345,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"serviceUserArn" must be a string/
            );
        });
    });

    describe('nodes validation', () => {
        it('should accept valid positive integer for nodes', () => {
            const config = {
                ...validConfig,
                nodes: 5,
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.nodes, 5);
        });

        it('should default nodes to 1 when not specified', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.nodes, 1);
        });

        it('should throw if nodes is negative', () => {
            const config = {
                ...validConfig,
                nodes: -1,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"nodes" must be a positive number/
            );
        });

        it('should throw if nodes is zero', () => {
            const config = {
                ...validConfig,
                nodes: 0,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"nodes" must be a positive number/
            );
        });

        it('should throw if nodes is not an integer', () => {
            const config = {
                ...validConfig,
                nodes: 2.5,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"nodes" must be an integer/
            );
        });

        it('should throw if nodes is not a number', () => {
            const config = {
                ...validConfig,
                nodes: 'two',
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"nodes" must be a number/
            );
        });
    });

    describe('tokenBucketBufferSize validation', () => {
        it('should use default tokenBucketBufferSize when not specified', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.tokenBucketBufferSize, 50);
        });

        it('should accept custom tokenBucketBufferSize', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                tokenBucketBufferSize: 100,
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.tokenBucketBufferSize, 100);
        });

        it('should throw if tokenBucketBufferSize is not a positive integer', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                tokenBucketBufferSize: -10,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"tokenBucketBufferSize" must be a positive number/
            );
        });

        it('should throw if tokenBucketBufferSize is zero', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                tokenBucketBufferSize: 0,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"tokenBucketBufferSize" must be a positive number/
            );
        });

        it('should throw if tokenBucketBufferSize is not an integer', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                tokenBucketBufferSize: 50.5,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"tokenBucketBufferSize" must be an integer/
            );
        });
    });

    describe('tokenBucketRefillThreshold validation', () => {
        it('should use default tokenBucketRefillThreshold when not specified', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.tokenBucketRefillThreshold, 20);
        });

        it('should accept custom tokenBucketRefillThreshold', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                tokenBucketRefillThreshold: 30,
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.tokenBucketRefillThreshold, 30);
        });

        it('should throw if tokenBucketRefillThreshold is not a positive integer', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                tokenBucketRefillThreshold: -5,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"tokenBucketRefillThreshold" must be a positive number/
            );
        });

        it('should throw if tokenBucketRefillThreshold is zero', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                tokenBucketRefillThreshold: 0,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"tokenBucketRefillThreshold" must be a positive number/
            );
        });

        it('should throw if tokenBucketRefillThreshold is not an integer', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                tokenBucketRefillThreshold: 20.5,
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"tokenBucketRefillThreshold" must be an integer/
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket" must be of type object/
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

            const result = parseRateLimitConfig(config);

            assert(result.bucket.defaultConfig);
            assert(result.bucket.defaultConfig.RequestsPerSecond);
        });

        it('should throw if defaultConfig is not an object', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: 'invalid',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket.defaultConfig" must be of type object/
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket.defaultConfig.requestsPerSecond" must be of type object/
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

            const result = parseRateLimitConfig(config);
            // limit = 0 means unlimited, should be accepted
            assert(result.bucket.defaultConfig.RequestsPerSecond);
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
                () => parseRateLimitConfig(config),
                // eslint-disable-next-line max-len
                /rateLimiting configuration is invalid.*"bucket.defaultConfig.requestsPerSecond.limit" must be larger than or equal to 0/
            );
        });

        it('should throw if limit is missing when requestsPerSecond is provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            burstCapacity: 10,
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket.defaultConfig.requestsPerSecond.limit" is required/
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

            const result = parseRateLimitConfig(config);
            assert.strictEqual(
                result.bucket.defaultConfig.RequestsPerSecond.BurstCapacity,
                constants.rateLimitDefaultBurstCapacity
            );
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

            const result = parseRateLimitConfig(config);
            assert.strictEqual(
                result.bucket.defaultConfig.RequestsPerSecond.BurstCapacity, 20
            );
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
                () => parseRateLimitConfig(config),
                // eslint-disable-next-line max-len
                /rateLimiting configuration is invalid.*"bucket.defaultConfig.requestsPerSecond.burstCapacity" must be larger than or equal to 0/
            );
        });

        it('should accept zero burstCapacity', () => {
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

            const result = parseRateLimitConfig(config);
            assert.strictEqual(
                result.bucket.defaultConfig.RequestsPerSecond.BurstCapacity, 0
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
                () => parseRateLimitConfig(config),
                // eslint-disable-next-line max-len
                /rateLimiting configuration is invalid.*"bucket.defaultConfig.requestsPerSecond.burstCapacity" must be a number/
            );
        });

        it('should accept float burstCapacity', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 1.5,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(
                result.bucket.defaultConfig.RequestsPerSecond.BurstCapacity, 1.5
            );
        });

        it('should accept zero defaultBurstCapacity and apply it when burstCapacity is omitted', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                        },
                    },
                    defaultBurstCapacity: 0,
                },
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.bucket.defaultBurstCapacity, 0);
            assert.strictEqual(
                result.bucket.defaultConfig.RequestsPerSecond.BurstCapacity, 0
            );
        });

        it('should throw if defaultBurstCapacity is negative', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultBurstCapacity: -1,
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket.defaultBurstCapacity" must be larger than or equal to 0/
            );
        });

        it('should accept float defaultBurstCapacity', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultBurstCapacity: 1.5,
                },
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(result.bucket.defaultBurstCapacity, 1.5);
            assert.strictEqual(
                result.bucket.defaultConfig.RequestsPerSecond.BurstCapacity, 1.5
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

            const result = parseRateLimitConfig(config);
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket.configCacheTTL" must be a positive number/
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket.configCacheTTL" must be a positive number/
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket.configCacheTTL" must be an integer/
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket.configCacheTTL" must be a number/
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"error" must be of type object/
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

            const result4xx = parseRateLimitConfig(config4xx);
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

            const result5xx = parseRateLimitConfig(config5xx);
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"error.statusCode" must be larger than or equal to 400/
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"error.statusCode" must be less than or equal to 599/
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"error.statusCode" must be an integer/
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"error.statusCode" must be a number/
            );
        });

        it('should use default message when message is not provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 503,
                },
            };

            const result = parseRateLimitConfig(config);
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"error.message" must be a string/
            );
        });

        it('should use SlowDown error when no error config is provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
            };

            const result = parseRateLimitConfig(config);
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

            const result = parseRateLimitConfig(config);
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

            const result = parseRateLimitConfig(config);
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
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"error.code" must be a string/
            );
        });
    });

    describe('distributed rate limiting validation', () => {
        it('should validate limit against nodes', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                nodes: 5,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 3, // Less than 5 nodes
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /requestsPerSecond\.limit \(3\) must be >= nodes \(5\)/
            );
        });
    });

    describe('account-level rate limiting', () => {
        it('should parse account configuration similar to bucket', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 500,
                            burstCapacity: 5,
                        },
                    },
                    configCacheTTL: 60000,
                    defaultBurstCapacity: 2,
                },
            };

            const result = parseRateLimitConfig(config);

            assert(result.account);
            assert(result.account.defaultConfig);
            assert.strictEqual(result.account.defaultConfig.RequestsPerSecond.Limit, 500);
            assert.strictEqual(result.account.configCacheTTL, 60000);
            assert.strictEqual(result.account.defaultBurstCapacity, 2);
        });

        it('should support both bucket and account rate limiting simultaneously', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 1000,
                        },
                    },
                },
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 500,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);

            assert(result.bucket.defaultConfig);
            assert.strictEqual(result.bucket.defaultConfig.RequestsPerSecond.Limit, 1000);
            assert(result.account.defaultConfig);
            assert.strictEqual(result.account.defaultConfig.RequestsPerSecond.Limit, 500);
        });

        it('should validate account limit against nodes', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                nodes: 10,
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 7, // Less than 10 nodes
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /requestsPerSecond\.limit \(7\) must be >= nodes \(10\)/
            );
        });

        it('should throw if account is not an object', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                account: 'invalid',
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"account" must be of type object/
            );
        });

        it('should apply defaults to account config when fields are omitted', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);

            assert.strictEqual(result.account.configCacheTTL, constants.rateLimitDefaultConfigCacheTTL);
            assert.strictEqual(result.account.defaultBurstCapacity, constants.rateLimitDefaultBurstCapacity);
        });
    });

    describe('account burstCapacity validation', () => {
        it('should use default burstCapacity when not provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(
                result.account.defaultConfig.RequestsPerSecond.BurstCapacity,
                constants.rateLimitDefaultBurstCapacity
            );
        });

        it('should use custom burstCapacity when provided', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 20,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(
                result.account.defaultConfig.RequestsPerSecond.BurstCapacity, 20
            );
        });

        it('should accept float burstCapacity', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 1.5,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(
                result.account.defaultConfig.RequestsPerSecond.BurstCapacity, 1.5
            );
        });

        it('should throw if burstCapacity is negative', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: -5,
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                // eslint-disable-next-line max-len
                /rateLimiting configuration is invalid.*"account.defaultConfig.requestsPerSecond.burstCapacity" must be larger than or equal to 0/
            );
        });

        it('should accept zero burstCapacity', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 0,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);
            assert.strictEqual(
                result.account.defaultConfig.RequestsPerSecond.BurstCapacity, 0
            );
        });

        it('should throw if burstCapacity is not a number', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                account: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                            burstCapacity: 'ten',
                        },
                    },
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                // eslint-disable-next-line max-len
                /rateLimiting configuration is invalid.*"account.defaultConfig.requestsPerSecond.burstCapacity" must be a number/
            );
        });
    });

    describe('schema validation - unknown fields', () => {
        it('should reject unknown top-level fields', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                unknownField: 'invalid',
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"unknownField" is not allowed/
            );
        });

        it('should reject unknown fields in bucket config', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                bucket: {
                    unknownField: 'invalid',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"bucket.unknownField" is not allowed/
            );
        });

        it('should reject unknown fields in error config', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                error: {
                    statusCode: 503,
                    unknownField: 'invalid',
                },
            };

            assert.throws(
                () => parseRateLimitConfig(config),
                /rateLimiting configuration is invalid.*"error.unknownField" is not allowed/
            );
        });
    });

    describe('schema validation - multiple errors', () => {
        it('should report all validation errors at once', () => {
            const config = {
                // Missing serviceUserArn (required)
                nodes: -5, // Invalid (must be positive)
                tokenBucketBufferSize: 0, // Invalid (must be positive)
                bucket: 'not an object', // Invalid type
            };

            assert.throws(() => {
                try {
                    parseRateLimitConfig(config);
                } catch (error) {
                    // Should contain multiple errors
                    assert(error.message.includes('serviceUserArn'));
                    assert(error.message.includes('nodes'));
                    assert(error.message.includes('tokenBucketBufferSize'));
                    assert(error.message.includes('bucket'));
                    throw error;
                }
            }, /rateLimiting configuration is invalid/);
        });
    });

    describe('calculation verification', () => {
        it('should store Limit in defaultConfig for distributed setup', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                nodes: 2,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 100,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);

            assert.strictEqual(result.bucket.defaultConfig.RequestsPerSecond.Limit, 100);
            assert.strictEqual(
                result.bucket.defaultConfig.RequestsPerSecond.BurstCapacity,
                constants.rateLimitDefaultBurstCapacity
            );
        });

        it('should store BurstCapacity from burstCapacity', () => {
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

            const result = parseRateLimitConfig(config);

            assert.strictEqual(result.bucket.defaultConfig.RequestsPerSecond.BurstCapacity, 15);
        });

        it('should handle single node setup', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                nodes: 1,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 50,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);

            assert.strictEqual(result.bucket.defaultConfig.RequestsPerSecond.Limit, 50);
        });

        it('should handle high-scale distributed setup', () => {
            const config = {
                serviceUserArn: 'arn:aws:iam::123456789012:user/rate-limit-service',
                nodes: 10,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: {
                            limit: 10000,
                            burstCapacity: 5,
                        },
                    },
                },
            };

            const result = parseRateLimitConfig(config);

            assert.strictEqual(result.bucket.defaultConfig.RequestsPerSecond.Limit, 10000);
            assert.strictEqual(result.bucket.defaultConfig.RequestsPerSecond.BurstCapacity, 5);
        });
    });
});
