describe('Config', () => {
    it('should load default config.json without errors', done => {
        require('../../lib/Config');
        done();
    });
});
