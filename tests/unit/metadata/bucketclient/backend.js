const assert = require('assert');
const http = require('http');
const BucketClientInterface =
    require('../../../../lib/metadata/bucketclient/backend');
const { DummyRequestLogger } =
    require('../../helpers.js');

const bucketName = 'test';
const invalidBucket = 'invalid';
const params = {};
const logger = new DummyRequestLogger();
const port = 9000;

let bucketclient;
let server;

const dataJSON = { message: 'This is Correct!!' };

function makeResponse(res, code, message, data, md) {
    /* eslint-disable no-param-reassign */
    res.statusCode = code;
    res.statusMessage = message;
    /* eslint-disable no-param-reassign */
    if (md) {
        res.setHeader('x-scal-usermd', md);
    }
    if (data) {
        res.write(JSON.stringify(data));
    }
    res.end();
}

function handler(req, res) {
    const { method, url } = req;
    const key = url.split('?')[0];
    if (method === 'GET' && key === `/default/bucket/${bucketName}`) {
        makeResponse(res, 200, 'This is GET', dataJSON);
    } else {
        res.statusCode = 404;
        res.end();
    }
}

describe('BucketFileInteraface::listMultipartUploads', () => {
    before('Creating Server', done => {
        bucketclient = new BucketClientInterface();
        server = http.createServer(handler).listen(port);
        server.on('listening', () => {
            done();
        });
        server.on('error', err => {
            process.stdout.write(`${err.stack}\n`);
            process.exit(1);
        });
    });

    after('Terminating Server', () => {
        server.close();
    });

    it('Error handling - Error Case', done => {
        bucketclient.listMultipartUploads(
            invalidBucket,
            params,
            logger,
            (err, data) => {
                assert.strictEqual(err.description, 'unexpected error',
                    'Expected an error, but got success');
                assert.strictEqual(data, undefined, 'Data should be undefined');
                done();
            }
        );
    });

    it('Error handling - Success Case', done => {
        bucketclient.listMultipartUploads(
            bucketName,
            params,
            logger,
            (err, data) => {
                assert.equal(err, null,
                    `Expected success, but got error ${err}`);
                assert.deepStrictEqual(data, dataJSON,
                    'Expected some data, but got empty');
                done();
            }
        );
    });
});
