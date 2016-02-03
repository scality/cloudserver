import Config from '../../lib/Config';

describe('Config', () => {
    it('should load default config.json without errors', () => {
        new Config(); // eslint-disable-line no-new
    });
});
