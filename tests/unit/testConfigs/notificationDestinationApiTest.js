const assert = require('assert');
const sinon = require('sinon');
const fs = require('fs');
const path = require('path');
const { ConfigObject } = require('../../../lib/Config');
const { parseNotificationDestinationApiConfig } = require('../../../lib/api/apiUtils/notificationDestination/config');

const envVar = 'S3_NOTIFICATION_DESTINATION_API_ENABLED';
const bootstrapError = /bad config: notificationDestinationApi: "bucketd.bootstrap/;
const missingBootstrapError = /bad config: notificationDestinationApi.bucketd.bootstrap is required/;

describe('parseNotificationDestinationApiConfig', () => {
    let savedEnv;

    beforeEach(() => {
        savedEnv = process.env[envVar];
        delete process.env[envVar];
    });

    afterEach(() => {
        if (savedEnv === undefined) {
            delete process.env[envVar];
        } else {
            process.env[envVar] = savedEnv;
        }
    });

    const bucketd = { bootstrap: ['localhost:9000'] };

    describe('on the scality backend', () => {
        const parse = block => parseNotificationDestinationApiConfig(block, 'scality');

        it('should be off when the block is missing', () => {
            assert.deepStrictEqual(parse(undefined), { enabled: false, bucketd: null });
        });

        it('should be off when enabled is not set', () => {
            assert.deepStrictEqual(parse({ bucketd }), { enabled: false, bucketd: null });
        });

        it('should be on with a bootstrap list', () => {
            assert.deepStrictEqual(parse({ enabled: true, bucketd }), {
                enabled: true,
                bucketd: { bootstrap: ['localhost:9000'] },
            });
        });

        it('should throw when the block is not an object', () => {
            assert.throws(() => parse('yes'), /bad config: notificationDestinationApi: "value" must be of type object/);
        });

        it('should throw when enabled is not a boolean', () => {
            assert.throws(
                () => parse({ enabled: 'true', bucketd }),
                /bad config: notificationDestinationApi: "enabled" must be a boolean/,
            );
        });

        // A misspelled or unknown key must not be ignored in silence
        [
            ['active', { enabled: true, active: false, bucketd }, 'active'],
            ['a misspelled enabled', { enable: false }, 'enable'],
        ].forEach(([desc, block, key]) => {
            it(`should throw on an unknown key: ${desc}`, () => {
                assert.throws(
                    () => parse(block),
                    new RegExp(`bad config: notificationDestinationApi: "${key}" is not allowed`),
                );
            });
        });

        it('should throw on an unknown key in the bucketd block', () => {
            assert.throws(
                () => parse({ enabled: true, bucketd: { bootstarp: ['localhost:9000'] } }),
                /bad config: notificationDestinationApi: "bucketd.bootstarp" is not allowed/,
            );
        });

        it('should let the environment turn it on', () => {
            process.env[envVar] = 'true';
            assert.strictEqual(parse({ enabled: false, bucketd }).enabled, true);
        });

        it('should let the environment turn it off', () => {
            process.env[envVar] = 'false';
            assert.deepStrictEqual(parse({ enabled: true, bucketd }), { enabled: false, bucketd: null });
        });

        it('should throw on an environment value other than true or false', () => {
            process.env[envVar] = 'yes';
            assert.throws(() => parse({ bucketd }), new RegExp(`${envVar} must be`));
        });

        [
            ['empty', { bootstrap: [] }],
            ['not a list', { bootstrap: 'localhost:9000' }],
            ['not strings', { bootstrap: [9000] }],
            ['an empty string', { bootstrap: [''] }],
        ].forEach(([desc, block]) => {
            it(`should throw when the bootstrap list is ${desc}`, () => {
                assert.throws(() => parse({ enabled: true, bucketd: block }), bootstrapError);
            });

            it(`should not read a bootstrap list that is ${desc} when off`, () => {
                assert.deepStrictEqual(parse({ enabled: false, bucketd: block }), { enabled: false, bucketd: null });
            });
        });

        it('should not read the bucketd block when off', () => {
            assert.deepStrictEqual(parse({ enabled: false, bucketd: { bootstrap: 'nonsense' } }), {
                enabled: false,
                bucketd: null,
            });
        });

        it('should throw on an unknown key in the bucketd block when off', () => {
            assert.throws(
                () => parse({ enabled: false, bucketd: { bootstarp: ['localhost:9000'] } }),
                /bad config: notificationDestinationApi: "bucketd.bootstarp" is not allowed/,
            );
        });

        [
            ['a string', 'x'],
            ['null', null],
            ['an array', []],
        ].forEach(([desc, block]) => {
            it(`should throw when the bucketd block is ${desc}, even when off`, () => {
                assert.throws(
                    () => parse({ enabled: false, bucketd: block }),
                    /bad config: notificationDestinationApi: "bucketd" must be of type object/,
                );
            });
        });

        it('should accept any non-empty strings, as bucketd.bootstrap does', () => {
            const bootstrap = ['bucketd_1:9000', '10.0.0.1:9000', '[::1]:9000'];
            assert.deepStrictEqual(parse({ enabled: true, bucketd: { bootstrap } }).bucketd.bootstrap, bootstrap);
        });

        [
            ['the bucketd block is missing', undefined],
            ['the bootstrap list is missing', {}],
        ].forEach(([desc, block]) => {
            it(`should throw when enabled and ${desc}`, () => {
                assert.throws(() => parse({ enabled: true, bucketd: block }), missingBootstrapError);
            });
        });
    });

    ['mem', 'file'].forEach(metadata => {
        describe(`on the ${metadata} backend`, () => {
            const parse = block => parseNotificationDestinationApiConfig(block, metadata);

            it('should be on with no bucketd block', () => {
                assert.deepStrictEqual(parse({ enabled: true }), { enabled: true, bucketd: null });
            });

            it('should not read the bucketd block', () => {
                assert.deepStrictEqual(parse({ enabled: true, bucketd: { bootstrap: [] } }), {
                    enabled: true,
                    bucketd: null,
                });
            });

            it('should not read the bucketd block when off', () => {
                assert.deepStrictEqual(parse({ enabled: false, bucketd: { bootstrap: 'nonsense' } }), {
                    enabled: false,
                    bucketd: null,
                });
            });

            it('should throw on an unknown key in the bucketd block', () => {
                assert.throws(
                    () => parse({ enabled: false, bucketd: { bootstarp: ['localhost:9000'] } }),
                    /bad config: notificationDestinationApi: "bucketd.bootstarp" is not allowed/,
                );
            });

            it('should let the environment turn it on', () => {
                process.env[envVar] = 'true';
                assert.deepStrictEqual(parse({ enabled: false }), { enabled: true, bucketd: null });
            });

            it('should throw on an unknown key', () => {
                assert.throws(
                    () => parse({ enabled: true, active: true }),
                    /bad config: notificationDestinationApi: "active" is not allowed/,
                );
            });
        });
    });

    describe('on the mongodb backend', () => {
        const parse = block => parseNotificationDestinationApiConfig(block, 'mongodb');
        let emitWarning;

        beforeEach(() => {
            emitWarning = sinon.stub(process, 'emitWarning');
        });

        afterEach(() => {
            emitWarning.restore();
        });

        [
            ['the block is missing', undefined],
            ['the flag is off with a malformed bucketd block', { enabled: false, bucketd: 'nonsense' }],
            ['the block is not an object', 'yes'],
            ['enabled is not a boolean', { enabled: 'yes' }],
            ['config.json requests the flag', { enabled: true }],
        ].forEach(([desc, block]) => {
            it(`should never throw and stay off, with no warning, when ${desc}`, () => {
                assert.deepStrictEqual(parse(block), { enabled: false, bucketd: null });
                assert.strictEqual(emitWarning.called, false);
            });
        });

        ['true', 'yes'].forEach(value => {
            it(`should stay off, with no warning, when the environment says ${value}`, () => {
                process.env[envVar] = value;
                assert.deepStrictEqual(parse({ enabled: true }), { enabled: false, bucketd: null });
                assert.strictEqual(emitWarning.called, false);
            });
        });
    });

    describe('on the cdmi backend', () => {
        it('should stay off when config.json requests the flag', () => {
            assert.deepStrictEqual(parseNotificationDestinationApiConfig({ enabled: true }, 'cdmi'), {
                enabled: false,
                bucketd: null,
            });
        });
    });
});

describe('notificationDestinationApi in the config', () => {
    // The tests read the shipped config.json. Thus, before each test, remove
    // any config file that the environment sets, and restore it after the test.
    const vars = [envVar, 'S3METADATA', 'S3_CONFIG_FILE'];
    let saved;

    beforeEach(() => {
        saved = {};
        vars.forEach(key => {
            saved[key] = process.env[key];
            delete process.env[key];
        });
    });

    afterEach(() => {
        vars.forEach(key => {
            if (saved[key] === undefined) {
                delete process.env[key];
            } else {
                process.env[key] = saved[key];
            }
        });
    });

    it('should ship off, with no address', () => {
        const shipped = JSON.parse(fs.readFileSync(path.join(__dirname, '../../../config.json'), 'utf-8'));
        assert.deepStrictEqual(shipped.notificationDestinationApi, { enabled: false });
        const config = new ConfigObject();
        assert.strictEqual(config.notificationDestinationApi.enabled, false);
    });

    it('should refuse the scality backend when enabled with no bootstrap list', () => {
        process.env[envVar] = 'true';
        process.env.S3METADATA = 'scality';
        assert.throws(() => new ConfigObject(), missingBootstrapError);
    });

    ['mem', 'file'].forEach(metadata => {
        it(`should accept the ${metadata} backend when enabled with no bootstrap list`, () => {
            process.env[envVar] = 'true';
            process.env.S3METADATA = metadata;
            const config = new ConfigObject();
            assert.strictEqual(config.backends.metadata, metadata);
            assert.deepStrictEqual(config.notificationDestinationApi, { enabled: true, bucketd: null });
        });
    });

    it('should start on the mongodb backend when enabled, with the flag off', () => {
        process.env[envVar] = 'true';
        process.env.S3METADATA = 'mongodb';
        const config = new ConfigObject();
        assert.strictEqual(config.backends.metadata, 'mongodb');
        assert.deepStrictEqual(config.notificationDestinationApi, { enabled: false, bucketd: null });
    });
});
