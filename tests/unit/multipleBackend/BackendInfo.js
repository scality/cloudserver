const assert = require('assert');
const { BackendInfo } = require('../../../lib/api/apiUtils/object/BackendInfo');
const { DummyRequestLogger } = require('../helpers');
const { config } = require('../../../lib/Config');

const log = new DummyRequestLogger();
const data = config.backends.data;

const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const dummyBackendInfo = new BackendInfo(memLocation, fileLocation,
    '127.0.0.1');

describe('BackendInfo class', () => {
    describe('controllingBackendParam', () => {
        beforeEach(() => {
            config.backends.data = data;
        });
        it('should return object with applicable error if ' +
        'objectLocationConstraint is invalid', () => {
            const res = BackendInfo.controllingBackendParam(
                'notValid', fileLocation, '127.0.0.1', log);
            assert.equal(res.isValid, false);
            assert((res.description).indexOf('Object Location Error')
            > -1);
        });
        it('should return object with applicable error if ' +
        'bucketLocationConstraint is invalid and no ' +
        'objectLocationConstraint was provided', () => {
            const res = BackendInfo.controllingBackendParam(
                undefined, 'notValid', '127.0.0.1', log);
            assert.equal(res.isValid, false);
            assert((res.description).indexOf('Bucket ' +
            'Location Error') > -1);
        });
        it('should return object with applicable error if requestEndpoint ' +
        'is invalid, no objectLocationConstraint or bucketLocationConstraint' +
        'was provided and data backend is set to "scality"', () => {
            config.backends.data = 'scality';
            const res = BackendInfo.controllingBackendParam(
                undefined, undefined, 'notValid', log);
            assert.equal(res.isValid, false);
            assert((res.description).indexOf('Endpoint ' +
            'Location Error') > -1);
        });
        it('should return object with applicable error if requestEndpoint ' +
        'is invalid, no objectLocationConstraint or ' +
        'bucketLocationConstraint was provided and ' +
        'data backend is set to "multiple"', () => {
            config.backends.data = 'multiple';
            const res = BackendInfo.controllingBackendParam(
                undefined, undefined, 'notValid', log);
            assert.equal(res.isValid, false);
            assert((res.description).indexOf('Endpoint ' +
            'Location Error') > -1);
        });
        it('should return isValid if requestEndpoint ' +
        'is invalid and data backend is set to "file"', () => {
            config.backends.data = 'file';
            const res = BackendInfo.controllingBackendParam(
                memLocation, fileLocation, 'notValid', log);
            assert.equal(res.isValid, true);
        });
        it('should return isValid if requestEndpoint ' +
        'is invalid and data backend is set to "mem"', () => {
            config.backends.data = 'mem';
            const res = BackendInfo.controllingBackendParam(
                memLocation, fileLocation, 'notValid', log);
            assert.equal(res.isValid, true);
        });
        it('should return isValid if requestEndpoint ' +
        'is invalid but valid objectLocationConstraint' +
        'was provided', () => {
            config.backends.data = 'multiple';
            const res = BackendInfo.controllingBackendParam(
                memLocation, undefined, 'notValid', log);
            assert.equal(res.isValid, true);
        });
        it('should return isValid if requestEndpoint ' +
        'is invalid but valid bucketLocationConstraint' +
        'was provided', () => {
            config.backends.data = 'multiple';
            const res = BackendInfo.controllingBackendParam(
                undefined, memLocation, 'notValid', log);
            assert.equal(res.isValid, true);
        });
        it('should return isValid if all backend ' +
        'parameters are valid', () => {
            const res = BackendInfo.controllingBackendParam(
                memLocation, fileLocation, '127.0.0.1', log);
            assert.equal(res.isValid, true);
        });
    });
    describe('getControllingLocationConstraint', () => {
        it('should return object location constraint', () => {
            const controllingLC =
                dummyBackendInfo.getControllingLocationConstraint();
            assert.strictEqual(controllingLC, memLocation);
        });
    });
    describe('getters', () => {
        it('should return object location constraint', () => {
            const objectLC =
                dummyBackendInfo.getObjectLocationConstraint();
            assert.strictEqual(objectLC, memLocation);
        });
        it('should return bucket location constraint', () => {
            const bucketLC =
                dummyBackendInfo.getBucketLocationConstraint();
            assert.strictEqual(bucketLC, fileLocation);
        });
        it('should return request endpoint', () => {
            const reqEndpoint =
                dummyBackendInfo.getRequestEndpoint();
            assert.strictEqual(reqEndpoint, '127.0.0.1');
        });
    });
});
