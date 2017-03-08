import assert from 'assert';
import { BackendInfo } from '../../../lib/api/apiUtils/object/BackendInfo';
import { DummyRequestLogger } from '../helpers';

const log = new DummyRequestLogger();

const dummyBackendInfo = new BackendInfo('mem', 'file', '127.0.0.1');

describe('BackendInfo class', () => {
    describe('isValidControllingBackendParam', () => {
        it('should return false if objectLocationConstraint is invalid', () => {
            const res = BackendInfo.isValidControllingBackendParam(
                'notValid', 'file', '127.0.0.1', log);
            assert.equal(res, false);
        });
        it('should return false if bucketLocationConstraint is invalid', () => {
            const res = BackendInfo.isValidControllingBackendParam(
                'mem', 'notValid', '127.0.0.1', log);
            assert.equal(res, false);
        });
        it('should return false if requestEndpoint is invalid', () => {
            const res = BackendInfo.isValidControllingBackendParam(
                'mem', 'file', 'notValid', log);
            assert.equal(res, false);
        });
        it('should return true if all backend parameters are valid', () => {
            const res = BackendInfo.isValidControllingBackendParam(
                'mem', 'file', '127.0.0.1', log);
            assert.equal(res, true);
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
