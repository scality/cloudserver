const assert = require('assert');
const constants = require('../../../../constants');
const { makeS3Request } = require('../utils/makeRequest');

const bucket = 'testunsupportedqueriesbucket';
const objectKey = 'key';

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

describe('unsupported query requests:', () => {
    constants.unsupportedQueries.forEach(query => {
        const queryObj = {};
        queryObj[query] = '';

        itSkipIfAWS(`should respond with NotImplemented for ?${query} request`,
        done => {
            makeS3Request({ method: 'GET', queryObj, bucket, objectKey },
            err => {
                assert.strictEqual(err.code, 'NotImplemented');
                assert.strictEqual(err.statusCode, 501);
                done();
            });
        });
    });
});
