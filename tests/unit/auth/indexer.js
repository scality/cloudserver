import assert from 'assert';

import Indexer from '../../../lib/auth/in_memory/indexer';
import ref from '../../../conf/authdata.json';
import { should } from './checker';

describe('lib/auth/in_memory/index.js', () => {
    let obj = {};
    let index = undefined;

    beforeEach(done => {
        obj = JSON.parse(JSON.stringify(ref));
        index = new Indexer(obj);
        done();
    });

    it('Should return account from canonicalID', done => {
        const res = index.getByCanId(obj.accounts[0].canonicalID);
        assert.strictEqual(typeof res, 'object');
        assert.strictEqual(res.arn, obj.accounts[0].arn);
        done();
    });

    it('Should return account from email', done => {
        const res = index.getByEmail(obj.accounts[1].email);
        assert.strictEqual(typeof res, 'object');
        assert.strictEqual(res.canonicalID, obj.accounts[1].canonicalID);
        done();
    });

    it('Should return user from email', done => {
        const res = index.getByEmail(obj.accounts[0].users[0].email);
        assert.strictEqual(typeof res, 'object');
        assert.strictEqual(res.arn, obj.accounts[0].arn);
        assert.strictEqual(res.IAMdisplayName,
            obj.accounts[0].users[0].name);
        done();
    });

    it('Should return account from key', done => {
        const res = index.getByKey(obj.accounts[0].keys[0].access);
        assert.strictEqual(typeof res, 'object');
        assert.strictEqual(res.arn, obj.accounts[0].arn);
        done();
    });

    it('Should return user from key', done => {
        const res = index.getByKey(obj.accounts[0].users[0].keys[0].access);
        assert.strictEqual(typeof res, 'object');
        assert.strictEqual(res.arn, obj.accounts[0].arn);
        assert.strictEqual(res.IAMdisplayName,
            obj.accounts[0].users[0].name);
        done();
    });

    it('should index account without keys', done => {
        should._exec = () => {
            index = new Indexer(obj);
            const res = index.getByEmail(obj.accounts[0].email);
            assert.strictEqual(typeof res, 'object');
            assert.strictEqual(res.arn, obj.accounts[0].arn);
            done();
        };
        should.missingField(obj, 'accounts.0.keys');
    });

    it('should index account without users', done => {
        should._exec = () => {
            index = new Indexer(obj);
            const res = index.getByEmail(obj.accounts[0].email);
            assert.strictEqual(typeof res, 'object');
            assert.strictEqual(res.arn, obj.accounts[0].arn);
            done();
        };
        should.missingField(obj, 'accounts.0.users');
    });
});
