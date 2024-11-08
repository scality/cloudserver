const assert = require('assert');
const sinon = require('sinon');
const Cache = require('../../../lib/kms/Cache');

describe('Cache Class', () => {
    let cache;
    let sandbox;

    beforeEach(() => {
        cache = new Cache();
        sandbox = sinon.createSandbox();
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe('getResult()', () => {
        it('should return null when no result is set', () => {
            assert.strictEqual(cache.getResult(), null);
        });

        it('should return the cached result after setResult is called', () => {
            const fakeTimestamp = 1625077800000;
            sandbox.stub(Date, 'now').returns(fakeTimestamp);
            const result = { data: 'test' };
            cache.setResult(result);
            const expected = Object.assign({}, result, {
                lastChecked: new Date(fakeTimestamp).toISOString(),
            });
            assert.deepStrictEqual(cache.getResult(), expected);
        });
    });

    describe('getLastChecked()', () => {
        it('should return null when cache has never been set', () => {
            assert.strictEqual(cache.getLastChecked(), null);
        });

        it('should return the timestamp after setResult is called', () => {
            const fakeTimestamp = 1625077800000;
            sandbox.stub(Date, 'now').returns(fakeTimestamp);
            cache.setResult({ data: 'test' });
            assert.strictEqual(cache.getLastChecked(), fakeTimestamp);
        });
    });

    describe('setResult()', () => {
        it('should set the result and update lastChecked', () => {
            const fakeTimestamp = 1625077800000;
            sandbox.stub(Date, 'now').returns(fakeTimestamp);

            const result = { data: 'test' };
            cache.setResult(result);

            const expectedResult = Object.assign({}, result, {
                lastChecked: new Date(fakeTimestamp).toISOString(),
            });
            assert.deepStrictEqual(cache.getResult(), expectedResult);
            assert.strictEqual(cache.getLastChecked(), fakeTimestamp);
        });
    });

    describe('shouldRefresh()', () => {
        it('should return true if cache has never been set', () => {
            assert.strictEqual(cache.shouldRefresh(), true);
        });

        it('should return false if elapsed time is less than duration minus maximum jitter', () => {
            const fakeNow = 1625077800000;
            const fakeLastChecked = fakeNow - (45 * 60 * 1000); // 45 minutes ago
            sandbox.stub(Date, 'now').returns(fakeNow);
            sandbox.stub(Math, 'random').returns(0);
            cache.lastChecked = fakeLastChecked;

            // elapsed = 45 minutes, duration - jitter = 60 minutes
            // 45 < 60 => shouldRefresh = false
            assert.strictEqual(cache.shouldRefresh(), false);
        });

        it('should return true if elapsed time is greater than duration minus maximum jitter', () => {
            const fakeNow = 1625077800000;
            const fakeLastChecked = fakeNow - (61 * 60 * 1000); // 61 minutes ago
            sandbox.stub(Date, 'now').returns(fakeNow);
            sandbox.stub(Math, 'random').returns(0);
            cache.lastChecked = fakeLastChecked;

            // elapsed = 61 minutes, duration - jitter = 60 minutes
            // 61 > 60 => shouldRefresh = true
            assert.strictEqual(cache.shouldRefresh(), true);
        });

        it('should use custom duration if provided', () => {
            const customDuration = 6 * 60 * 60 * 1000; // 6 hours in milliseconds
            const fakeNow = 1625077800000;
            sandbox.stub(Date, 'now').returns(fakeNow);

            // Elapsed time = 5 hours
            const fakeLastChecked1 = fakeNow - (5 * 60 * 60 * 1000);
            cache.lastChecked = fakeLastChecked1;

            sandbox.stub(Math, 'random').returns(0);

            // 5 hours < 6 hours => shouldRefresh = false
            assert.strictEqual(
                cache.shouldRefresh(customDuration),
                false,
                'Cache should not refresh within custom duration'
            );

            // Elapsed time = 7 hours
            const fakeLastChecked2 = fakeNow - (7 * 60 * 60 * 1000);
            cache.lastChecked = fakeLastChecked2;

            // 7 hours > 6 hours => shouldRefresh = true
            assert.strictEqual(
                cache.shouldRefresh(customDuration),
                true,
                'Cache should refresh after custom duration'
            );
        });
    });

    describe('clear()', () => {
        it('should reset lastChecked and result to null', () => {
            cache.setResult({ data: 'test' });
            cache.clear();
            assert.strictEqual(cache.getResult(), null);
            assert.strictEqual(cache.getLastChecked(), null);
        });
    });
});
