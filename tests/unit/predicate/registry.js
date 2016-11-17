import assert from 'assert';
import path from 'path';
import stream from 'stream';
import registry from '../../../lib/predicate/registry';
import { logger } from '../../../lib/utilities/logger';

describe('predicate.registry', () => {
    describe('put', () => {
        [{
            tag: 'a simple function',
            inp: {
                eventInfo: {
                    eventName: 'ObjectCreated:Put',
                    bucket: 'foo',
                },
                fn(params, callback) {
                    callback();
                },
            },
        }, {
            tag: 'from a path',
            inp: {
                eventInfo: {
                    eventName: 'ObjectCreated:Put',
                    bucket: 'foo',
                    prefix: 'blah/',
                },
                fn: path.join(__dirname, 'simpleHandler.js'),
            },
        }].forEach(t => {
            it(`should register ${t.tag} correctly`, done => {
                registry.purge();
                registry.put(t.inp.eventInfo, t.inp.fn, err => {
                    assert.ifError(err);
                    done();
                });
            });
        });
    });

    describe('run', () => {
        let request;
        let buf;
        beforeEach(done => {
            registry.purge();
            buf = new Buffer(JSON.stringify({
                type: 'FOOD',
                instance: 'hamburger',
            }));
            request = new stream.PassThrough();
            request.headers = {
                'content-type': 'application/json',
                'x-amz-meta-qid': 'Q3',
                'x-amz-meta-some-id': 'UxP6pZlH5xgFiZJAdVzrOw**',
                'x-amz-meta-some-other-id': '56',
                'x-amz-meta-platform': 'CI',
                'x-amz-meta-device': 'upload.js',
                'x-amz-meta-tool-version': '1.0.0',
                'x-amz-meta-use-google-vision': 'true',
            };
            request.bucketName = 'foo';
            request.objectKey = 'food.json';
            request.namespace = 'default';
            request.parsedHost = '127.0.0.1';
            request.parsedContentLength = buf.length;
            request.contentMD5 = '53cc928b5611140da65bd3b8a96394b7';
            request.end(buf);
            done();
        });

        it('should run a simple function correctly', done => {
            registry.put({
                eventName: 'ObjectCreated:Put',
                bucket: 'foo' },
                path.join(__dirname, 'simpleHandler.js'));
            registry.run({
                eventName: 'ObjectCreated:Put',
                request,
                log: logger,
            }, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should update metadata correctly', done => {
            registry.put({
                eventName: 'ObjectCreated:Put',
                bucket: 'foo' },
                path.join(__dirname, 'simpleHandler.js'));
            registry.run({
                eventName: 'ObjectCreated:Put',
                request,
                log: logger,
            }, err => {
                assert.ifError(err);
                const headers = request.headers;
                assert.strictEqual('passed',
                    headers['x-amz-meta-simple-handler']);
                assert.strictEqual('text/plain',
                    headers['content-type']);
                assert.strictEqual('food.txt', request.objectKey);
                done();
            });
        });

        it('should allow a predicate to reject the request', done => {
            registry.put({
                eventName: 'ObjectCreated:Put',
                bucket: 'foo' },
                (params, callback) => callback('FAIL'));
            registry.run({
                eventName: 'ObjectCreated:Put',
                request,
                log: logger,
            }, err => {
                assert.strictEqual(true, err.PreconditionFailed);
                done();
            });
        });

        it('should NOT handle error thrown by user code', done => {
            registry.put({
                eventName: 'ObjectCreated:Put',
                bucket: 'foo' },
                (params, callback) => {
                    if (!callback) {
                        return;
                    }
                    // NOTE: you can't catch this even in a test
                    //      This means when the user's code takes a poop
                    //      we drop the request. Just saying...
                    // process.nextTick(() => {
                    //     throw new TypeError('Boom!');
                    // });
                    throw new TypeError('Boom!');
                });
            assert.throws(() => {
                registry.run({
                    eventName: 'ObjectCreated:Put',
                    request,
                    log: logger,
                }, () => {
                    done(new Error('should never reach here'));
                });
            }, TypeError);
            process.nextTick(() => done());
        });

        it('should allow user to change the object data', done => {
            const userData = {
                type: 'FOOD',
                instance: 'pizza',
            };
            registry.put({
                eventName: 'ObjectCreated:Put',
                bucket: 'foo' },
                (params, callback) => {
                    const body = params.Records[0].s3.object.body;
                    body.setMode('transform');
                    const chunks = [];
                    body.on('data', d => chunks.push(d))
                    .on('end', () => {
                        const got = Buffer.concat(chunks);
                        assert.strictEqual(buf.toString(), got.toString());
                        body.end(JSON.stringify(userData));
                        callback();
                    });
                });
            registry.run({
                eventName: 'ObjectCreated:Put',
                bucket: 'foo',
                key: 'key1.json',
                request,
                log: logger,
            }, (err, output) => {
                assert.ifError(err);
                const chunks = [];
                output.on('data', d => chunks.push(d));
                output.on('end', () => {
                    const userBuf = Buffer.concat(chunks);
                    assert.equal(output.parsedContentLength, userBuf.length);
                    assert.deepEqual(userData, JSON.parse(userBuf.toString()));
                });
                done();
            });
        });
    });
});
