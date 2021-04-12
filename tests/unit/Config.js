const assert = require('assert');

describe('Config', () => {
    it('should load default config.json without errors', done => {
        require('../../lib/Config');
        done();
    });

    it('should emit an event when auth data is updated', done => {
        const { ConfigObject } = require('../../lib/Config');
        const config = new ConfigObject();
        let emitted = false;
        config.on('authdata-update', () => {
            emitted = true;
        });
        config.setAuthDataAccounts([]);
        if (emitted) {
            return done();
        }
        return done(new Error('authdata-update event was not emitted'));
    });

    describe('utapi option setup', () => {
        let oldConfig;

        before(() => {
            oldConfig = process.env.S3_CONFIG_FILE;
            process.env.S3_CONFIG_FILE =
                'tests/unit/testConfigs/allOptsConfig/config.json';
        });

        after(() => {
            process.env.S3_CONFIG_FILE = oldConfig;
        });

        it('should set up utapi local cache', () => {
            const { ConfigObject } = require('../../lib/Config');
            const config = new ConfigObject();

            assert.deepStrictEqual(
                config.localCache,
                { name: 'zenko', sentinels: [{ host: 'localhost', port: 6379 }] },
            );
            assert.deepStrictEqual(
                config.utapi.localCache,
                config.localCache,
            );
        });

        it('should set up utapi redis', () => {
            const { ConfigObject } = require('../../lib/Config');
            const config = new ConfigObject();

            assert.deepStrictEqual(
                config.redis,
                { name: 'zenko', sentinels: [{ host: 'localhost', port: 6379 }] },
            );
            assert.deepStrictEqual(
                config.utapi.redis,
                { host: 'localhost', port: 6379 },
            );
        });
    });
});
