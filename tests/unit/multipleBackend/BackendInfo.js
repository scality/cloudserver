import assert from 'assert';
import { BackendInfo } from '../../../lib/api/apiUtils/object/BackendInfo';
import { DummyRequestLogger } from '../helpers';

const log = new DummyRequestLogger();

const dummyBackendInfo = new BackendInfo('mem', 'file', '127.0.0.1');

describe('BackendInfo class', () => {
    describe('controllingBackendParam', () => {
        it('should return object with applicable error if ' +
        'objectLocationConstraint is invalid', () => {
            const res = BackendInfo.controllingBackendParam(
                'notValid', 'file', '127.0.0.1', log);
            assert.equal(res.isValid, false);
            assert((res.description).indexOf('Object Location Error')
            > -1);
        });
        it('should return object with applicable error if ' +
        'bucketLocationConstraint is invalid', () => {
            const res = BackendInfo.controllingBackendParam(
                'mem', 'notValid', '127.0.0.1', log);
            assert.equal(res.isValid, false);
            assert((res.description).indexOf('Bucket ' +
            'Location Error') > -1);
        });
        it('should return object with applicable error if requestEndpoint ' +
        'is invalid', () => {
            const res = BackendInfo.controllingBackendParam(
                'mem', 'file', 'notValid', log);
            assert.equal(res.isValid, false);
            assert((res.description).indexOf('Endpoint ' +
            'Location Error') > -1);
        });
        it('should return object with applicable error if all backend ' +
        'parameters are valid', () => {
            const res = BackendInfo.controllingBackendParam(
                'mem', 'file', '127.0.0.1', log);
            assert.equal(res.isValid, true);
        });
    });
    describe('getControllingLocationConstraint', () => {
        it('should return object location constraint', () => {
            const controllingLC =
                dummyBackendInfo.getControllingLocationConstraint();
            assert.strictEqual(controllingLC, 'mem');
        });
    });
    describe('getters', () => {
        it('should return object location constraint', () => {
            const objectLC =
                dummyBackendInfo.getObjectLocationConstraint();
            assert.strictEqual(objectLC, 'mem');
        });
        it('should return bucket location constraint', () => {
            const bucketLC =
                dummyBackendInfo.getBucketLocationConstraint();
            assert.strictEqual(bucketLC, 'file');
        });
        it('should return request endpoint', () => {
            const reqEndpoint =
                dummyBackendInfo.getRequestEndpoint();
            assert.strictEqual(reqEndpoint, '127.0.0.1');
        });
    });
});
