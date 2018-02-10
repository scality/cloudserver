const assert = require('assert');
const { errors } = require('arsenal');
const { JsonError, jsonRespCheck } =
    require('../../../lib/data/external/GCP').GcpUtils;

const error = errors.InternalError.customizeDescription(
    'error in JSON Request');
const errorResp = { statusMessage: 'unit test error', statusCode: 500 };
const errorBody = JSON.stringify({
    error: {
        code: 500,
        message: 'unit test error',
    },
});
const retError = new JsonError(errorResp.statusMessage, errorResp.statusCode);
const successResp = { statusCode: 200 };
const successObj = { Value: 'Success' };

describe('GcpUtils JSON API Helper Fucntions:', () => {
    it('should return InternalError if resp receives err is set', done => {
        jsonRespCheck(error, {}, 'Some body value', 'unitTest', err => {
            assert.deepStrictEqual(err, error);
            done();
        });
    });

    it('should return resp error if resp code is >= 300', done => {
        jsonRespCheck(null, errorResp, 'some body value', 'unitTest', err => {
            assert.deepStrictEqual(err, retError);
            done();
        });
    });

    it('should return error if body is a json error value', done => {
        jsonRespCheck(null, {}, errorBody, 'unitTest', err => {
            assert.deepStrictEqual(err, retError);
            done();
        });
    });

    it('should return success obj', done => {
        jsonRespCheck(null, successResp, JSON.stringify(successObj), 'unitTest',
        (err, res) => {
            assert.ifError(err, `Expected success, but got error ${err}`);
            assert.deepStrictEqual(res, successObj);
            done();
        });
    });

    it('should return no result if success resp but body is invalid', done => {
        jsonRespCheck(null, successResp, 'invalid body string', 'unitTest',
        (err, res) => {
            assert.ifError(err, `Expected success, but got error ${err}`);
            assert.strictEqual(res, undefined);
            done();
        });
    });
});
