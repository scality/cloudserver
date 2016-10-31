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
                bucket: 'foo',
                key: 'key1.json',
                request,
                log: logger,
            }, (err, output) => {
                assert.ifError(err);
                assert(output instanceof stream.Readable);
                done();
            });
        });

        it('should should handle error thrown by in user code', done => {
            registry.put({
                eventName: 'ObjectCreated:Put',
                bucket: 'foo' },
                (params, callback) => {
                    if (callback) {
                        throw new TypeError('Boom!');
                    }
                });
            registry.run({
                eventName: 'ObjectCreated:Put',
                bucket: 'foo',
                key: 'key1.json',
                request,
                log: logger,
            }, (err, output) => {
                assert.ok(err);
                assert.strictEqual(undefined, output);
                done();
            });
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
                    const data = params.Records[0].s3.object.data;
                    assert.strictEqual(buf.toString(), data.toString());
                    callback(null, 'changing uploaded data',
                      JSON.stringify(userData));
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
                output.on('data', c => chunks.push(c));
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
