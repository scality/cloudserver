const assert = require('assert');

const utils = require('../../lib/utils');

describe('utils.getAllEndpoints', () => {
    it('should return endpoints from config', () => {
        const allEndpoints = utils.getAllEndpoints();

        assert(allEndpoints.indexOf('127.0.0.1') >= 0);
        assert(allEndpoints.indexOf('s3.docker.test') >= 0);
        assert(allEndpoints.indexOf('127.0.0.2') >= 0);
        assert(allEndpoints.indexOf('s3.amazonaws.com') >= 0);
    });
});
