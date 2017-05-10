const assert = require('assert');
const httpMocks = require('node-mocks-http');
const { EventEmitter } = require('events');
const { errors } = require('arsenal');

const { cleanup, DummyRequestLogger } = require('../helpers');
const { ds } = require('../../../lib/data/in_memory/backend');
const routesUtils = require('../../../lib/routes/routesUtils');
const data = require('../../../lib/data/wrapper');

const responseStreamData = routesUtils.responseStreamData;
const log = new DummyRequestLogger();
const owner = 'accessKey1canonicalID';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const errCode = null;
const overrideHeaders = {};
const resHeaders = {};
const dataStoreEntry = {
    value: postBody,
    keyContext: {
        bucketName,
        owner,
        namespace,
    },
};

describe('responseStreamData:', () => {
    beforeEach(() => {
        cleanup();
    });

    it('should stream full requested object data for one part object', done => {
        ds.push(null, dataStoreEntry);
        const dataLocations = [{
            key: 1,
            dataStore: 'mem',
        }];
        const response = httpMocks.createResponse({
            eventEmitter: EventEmitter,
        });
        response.on('end', () => {
            const data = response._getData();
            assert.strictEqual(data, postBody.toString());
            done();
        });
        return responseStreamData(errCode, overrideHeaders,
            resHeaders, dataLocations, response, null, log);
    });

    it('should stream full requested object data for two part object', done => {
        ds.push(null, dataStoreEntry, dataStoreEntry);
        const dataLocations = [
            {
                key: 1,
                dataStore: 'mem',
                start: 0,
                size: 11,
            },
            {
                key: 2,
                dataStore: 'mem',
                start: 11,
                size: 11,
            }];
        const response = httpMocks.createResponse({
            eventEmitter: EventEmitter,
        });
        response.on('end', () => {
            const data = response._getData();
            const doublePostBody = postBody.toString().concat(postBody);
            assert.strictEqual(data, doublePostBody);
            done();
        });
        return responseStreamData(errCode, overrideHeaders,
            resHeaders, dataLocations, response, null, log);
    });

    it('#334 non-regression test, destroy connection on error', done => {
        const dataLocations = [{
            key: 1,
            dataStore: 'mem',
            start: 0,
            size: 11,
        }];
        const prev = data.get;
        data.get = (objectGetInfo, log, cb) => {
            setTimeout(() => cb(errors.InternalError), 1000);
        };
        const response = httpMocks.createResponse({
            eventEmitter: EventEmitter,
        });
        response.connection = {
            destroy: () => {
                data.get = prev;
                done();
            },
        };
        response.on('end', () => {
            data.get = prev;
            done(new Error('end reached instead of destroying connection'));
        });
        return responseStreamData(errCode, overrideHeaders,
            resHeaders, dataLocations, response, null, log);
    });
});
