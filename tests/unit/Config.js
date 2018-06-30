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
});
