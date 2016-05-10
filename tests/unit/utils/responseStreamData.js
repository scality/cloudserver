import assert from 'assert';
import httpMocks from 'node-mocks-http';
import { EventEmitter } from 'events';

import { cleanup, DummyRequestLogger } from '../helpers';
import { ds } from '../../../lib/data/in_memory/backend';
import routesUtils from '../../../lib/routes/routesUtils';

const responseStreamData = routesUtils.responseStreamData;
const log = new DummyRequestLogger();
const owner = 'accessKey1canonicalID';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = new Buffer('I am a body');
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

describe('responseStreamData function', () => {
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
});
