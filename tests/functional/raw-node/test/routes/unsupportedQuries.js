import assert from 'assert';
import constants from '../../../../../constants';
import { makeS3Request } from '../../utils/makeRequest';

const bucket = 'testunsupportedqueriesbucket';
const objectKey = 'key';

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

describe('unsupported query requests:', () => {
    Object.keys(constants.unsupportedQueries).forEach(query => {
        itSkipIfAWS(`should respond with NotImplemented for ?${query} request`,
        done => {
            const queryObj = {};
            queryObj[query] = '';
            makeS3Request({ method: 'GET', queryObj, bucket, objectKey },
            err => {
                assert.strictEqual(err.code, 'NotImplemented');
                assert.strictEqual(err.statusCode, 501);
                done();
            });
        });
    });

    itSkipIfAWS('should accept blacklisted query key as a query value ' +
    'to a query key that is not on the blacklist', done => {
        const queryObj = { test: Object.keys(constants.unsupportedQueries)[0] };
        makeS3Request({ method: 'GET', queryObj, bucket, objectKey }, err => {
            assert.strictEqual(err.code, 'NoSuchBucket');
            assert.strictEqual(err.statusCode, 404);
            done();
        });
    });
});
