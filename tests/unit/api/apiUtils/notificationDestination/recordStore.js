const assert = require('assert');
const sinon = require('sinon');
const { errors, storage, models } = require('@scality/arsenal');

const { config } = require('../../../../../lib/Config');
const constants = require('../../../../../constants');
const recordStore = require('../../../../../lib/api/apiUtils/notificationDestination/recordStore');
const { cleanup, DummyRequestLogger } = require('../../../helpers');

const { metadata } = storage.metadata.inMemory.metadata;
const { MetadataWrapper } = storage.metadata;

const { accountResourcesBucket: bucket, notificationDestinationKeyTag: tag, splitter } = constants;
const markerKey = `format${splitter}${tag}`;
const accountId = '123456789012';
const otherAccountId = '210987654321';
const keyOf = (account, name) => `${account}${splitter}${tag}${splitter}${name}`;

function makeRecord(account, name) {
    return {
        modelVersion: 1,
        Name: name,
        AccountId: account,
        Arn: `arn:scality:bucketnotif::${account}:${name}`,
        Type: 'kafka',
        Connection: { Host: 'broker1.example.com', Port: 9093, Topic: 'events' },
        Auth: { Type: 'none' },
        CreatedAt: '2026-11-05T09:30:00Z',
        UpdatedAt: '2026-11-05T09:30:00Z',
    };
}

// Resolve with [err, result] so that a failed assertion fails the test at
// once instead of in a metadata callback.
const call = (fn, ...args) => new Promise(resolve => fn(...args, (err, res) => resolve([err, res])));

describe('notification destination record store', () => {
    const log = new DummyRequestLogger();
    let savedBackend;

    // The store builds its client from the metadata backend of the config.
    // These tests read the in-memory store, so each test runs on mem,
    // whatever S3BACKEND says. stop() drops the client, so that each test
    // builds a client of its own.
    beforeEach(() => {
        savedBackend = config.backends.metadata;
        config.backends.metadata = 'mem';
        recordStore.stop();
        cleanup();
    });

    afterEach(() => {
        recordStore.stop();
        config.backends.metadata = savedBackend;
        sinon.restore();
    });

    describe('clientParams', () => {
        it('should connect to the record cluster, not the S3 metadata cluster, on scality', () => {
            const params = recordStore.clientParams({
                backends: { metadata: 'scality' },
                bucketd: { bootstrap: ['s3-cluster:9000'], log: { logLevel: 'info' } },
                https: { cert: 'cert' },
                notificationDestinationApi: { enabled: true, bucketd: { bootstrap: ['record-cluster:9000'] } },
            });
            assert.deepStrictEqual(params.bucketdBootstrap, ['record-cluster:9000']);
            assert.deepStrictEqual(params.bucketdLog, { logLevel: 'info' });
            assert.deepStrictEqual(params.https, { cert: 'cert' });
            assert.strictEqual(typeof params.customListingParser, 'function');
        });

        it('should take only the listing parser on mem', () => {
            const params = recordStore.clientParams({
                backends: { metadata: 'mem' },
                notificationDestinationApi: { enabled: true, bucketd: null },
            });
            assert.deepStrictEqual(Object.keys(params), ['customListingParser']);
        });

        it('should take the metadata server address on file, as the S3 metadata client does', () => {
            const { customListingParser, ...params } = recordStore.clientParams({
                backends: { metadata: 'file' },
                metadataClient: { host: 'md-host', port: 9990 },
                notificationDestinationApi: { enabled: true, bucketd: null },
            });
            assert.strictEqual(typeof customListingParser, 'function');
            assert.deepStrictEqual(params, {
                metadataClient: { host: 'md-host', port: 9990 },
                constants: { usersBucket: constants.usersBucket, splitter: constants.splitter },
                noDbOpen: null,
            });
        });
    });

    describe('putDestination', () => {
        it('should create the records bucket when it is missing', async () => {
            const record = makeRecord(accountId, 'orders');
            const [err] = await call(recordStore.putDestination, accountId, 'orders', record, log);
            assert.ifError(err);
            const bucketMD = metadata.buckets.get(bucket);
            assert(bucketMD, 'the records bucket should exist');
            assert.strictEqual(bucketMD.getOwner(), 'admin');
            assert.strictEqual(bucketMD.getOwnerDisplayName(), 'admin');
        });

        it('should store the record under the account and name key', async () => {
            const record = makeRecord(accountId, 'orders');
            const [err] = await call(recordStore.putDestination, accountId, 'orders', record, log);
            assert.ifError(err);
            assert.deepStrictEqual(metadata.keyMaps.get(bucket).get(keyOf(accountId, 'orders')), record);
        });

        it('should replace an existing record', async () => {
            const first = makeRecord(accountId, 'orders');
            const second = { ...makeRecord(accountId, 'orders'), UpdatedAt: '2026-11-06T00:00:00Z' };
            await call(recordStore.putDestination, accountId, 'orders', first, log);
            const [err] = await call(recordStore.putDestination, accountId, 'orders', second, log);
            assert.ifError(err);
            const [getErr, got] = await call(recordStore.getDestination, accountId, 'orders', log);
            assert.ifError(getErr);
            assert.deepStrictEqual(got, second);
        });

        it('should write once the bucket another request created in between', async () => {
            const client = recordStore.getClient();
            const bucketMD = new models.BucketInfo(bucket, 'admin', 'admin', new Date().toJSON());
            await call(client.createBucket.bind(client), bucket, bucketMD, log);
            const put = sinon.stub(client, 'putObjectMD');
            put.onFirstCall().callsArgWith(5, errors.NoSuchBucket);
            put.callThrough();
            const record = makeRecord(accountId, 'orders');
            const [err] = await call(recordStore.putDestination, accountId, 'orders', record, log);
            assert.ifError(err);
            assert.strictEqual(put.callCount, 2);
            assert.deepStrictEqual(metadata.keyMaps.get(bucket).get(keyOf(accountId, 'orders')), record);
        });

        it('should pass any other metadata error through', async () => {
            sinon.stub(recordStore.getClient(), 'putObjectMD').callsArgWith(5, errors.InternalError);
            const [err] = await call(
                recordStore.putDestination,
                accountId,
                'orders',
                makeRecord(accountId, 'orders'),
                log,
            );
            assert.strictEqual(err?.message, 'InternalError');
        });

        it('should pass a bucket creation error through', async () => {
            sinon.stub(recordStore.getClient(), 'createBucket').callsArgWith(3, errors.InternalError);
            const [err] = await call(
                recordStore.putDestination,
                accountId,
                'orders',
                makeRecord(accountId, 'orders'),
                log,
            );
            assert.strictEqual(err?.message, 'InternalError');
        });
    });

    describe('getDestination', () => {
        it('should return the stored record', async () => {
            const record = makeRecord(accountId, 'orders');
            await call(recordStore.putDestination, accountId, 'orders', record, log);
            const [err, got] = await call(recordStore.getDestination, accountId, 'orders', log);
            assert.ifError(err);
            assert.deepStrictEqual(got, record);
        });

        it('should answer NoSuchDestination when the records bucket is missing', async () => {
            const [err] = await call(recordStore.getDestination, accountId, 'orders', log);
            assert.strictEqual(err?.message, 'NoSuchDestination');
        });

        it('should answer NoSuchDestination when the name is unknown', async () => {
            await call(recordStore.putDestination, accountId, 'orders', makeRecord(accountId, 'orders'), log);
            const [err] = await call(recordStore.getDestination, accountId, 'other', log);
            assert.strictEqual(err?.message, 'NoSuchDestination');
        });

        it("should not return another account's record", async () => {
            await call(recordStore.putDestination, otherAccountId, 'orders', makeRecord(otherAccountId, 'orders'), log);
            const [err] = await call(recordStore.getDestination, accountId, 'orders', log);
            assert.strictEqual(err?.message, 'NoSuchDestination');
        });

        it('should pass any other metadata error through', async () => {
            sinon.stub(recordStore.getClient(), 'getObjectMD').callsArgWith(4, errors.InternalError);
            const [err] = await call(recordStore.getDestination, accountId, 'orders', log);
            assert.strictEqual(err?.message, 'InternalError');
        });
    });

    describe('deleteDestination', () => {
        it('should delete the record', async () => {
            await call(recordStore.putDestination, accountId, 'orders', makeRecord(accountId, 'orders'), log);
            const [err] = await call(recordStore.deleteDestination, accountId, 'orders', log);
            assert.ifError(err);
            const [getErr] = await call(recordStore.getDestination, accountId, 'orders', log);
            assert.strictEqual(getErr?.message, 'NoSuchDestination');
        });

        it('should answer NoSuchDestination when the records bucket is missing', async () => {
            const [err] = await call(recordStore.deleteDestination, accountId, 'orders', log);
            assert.strictEqual(err?.message, 'NoSuchDestination');
        });

        it('should answer NoSuchDestination when the name is unknown', async () => {
            await call(recordStore.putDestination, accountId, 'orders', makeRecord(accountId, 'orders'), log);
            const [err] = await call(recordStore.deleteDestination, accountId, 'other', log);
            assert.strictEqual(err?.message, 'NoSuchDestination');
        });

        it('should pass any other metadata error through', async () => {
            sinon.stub(recordStore.getClient(), 'deleteObjectMD').callsArgWith(4, errors.InternalError);
            const [err] = await call(recordStore.deleteDestination, accountId, 'orders', log);
            assert.strictEqual(err?.message, 'InternalError');
        });
    });

    describe('listDestinations', () => {
        async function putNames(account, names) {
            for (const name of names) {
                const [err] = await call(recordStore.putDestination, account, name, makeRecord(account, name), log);
                assert.ifError(err);
            }
        }

        it('should answer an empty list when the records bucket is missing', async () => {
            const [err, res] = await call(recordStore.listDestinations, accountId, { maxKeys: 100 }, log);
            assert.ifError(err);
            assert.deepStrictEqual(res, { destinations: [], isTruncated: false });
        });

        it('should list the names of the account only, without the key prefix', async () => {
            await putNames(accountId, ['b-orders', 'a-orders']);
            await putNames(otherAccountId, ['c-orders']);
            const [startErr] = await call(recordStore.start, log);
            assert.ifError(startErr);
            const [err, res] = await call(recordStore.listDestinations, accountId, { maxKeys: 100 }, log);
            assert.ifError(err);
            assert.deepStrictEqual(res, {
                destinations: [
                    { name: 'a-orders', record: makeRecord(accountId, 'a-orders') },
                    { name: 'b-orders', record: makeRecord(accountId, 'b-orders') },
                ],
                isTruncated: false,
            });
        });

        it('should page through nextMarker', async () => {
            await putNames(accountId, ['one', 'three', 'two']);
            const [err1, page1] = await call(recordStore.listDestinations, accountId, { maxKeys: 2 }, log);
            assert.ifError(err1);
            assert.deepStrictEqual(
                page1.destinations.map(d => d.name),
                ['one', 'three'],
            );
            assert.strictEqual(page1.isTruncated, true);
            assert.strictEqual(page1.nextMarker, 'three');

            const [err2, page2] = await call(
                recordStore.listDestinations,
                accountId,
                { marker: page1.nextMarker, maxKeys: 2 },
                log,
            );
            assert.ifError(err2);
            assert.deepStrictEqual(
                page2.destinations.map(d => d.name),
                ['two'],
            );
            assert.strictEqual(page2.isTruncated, false);
            assert.strictEqual(page2.nextMarker, undefined);
        });

        it('should not truncate a page that holds exactly maxKeys names', async () => {
            await putNames(accountId, ['one', 'two']);
            const [err, res] = await call(recordStore.listDestinations, accountId, { maxKeys: 2 }, log);
            assert.ifError(err);
            assert.strictEqual(res.destinations.length, 2);
            assert.strictEqual(res.isTruncated, false);
            assert.strictEqual(res.nextMarker, undefined);
        });

        it('should send no marker on the first page and a prefixed marker after', async () => {
            await putNames(accountId, ['one']);
            const list = sinon.spy(recordStore.getClient(), 'listObject');
            await call(recordStore.listDestinations, accountId, { maxKeys: 10 }, log);
            await call(recordStore.listDestinations, accountId, { marker: 'one', maxKeys: 10 }, log);
            const prefix = `${accountId}${splitter}${tag}${splitter}`;
            assert.strictEqual(list.firstCall.args[0], bucket);
            assert.strictEqual(list.firstCall.args[1].prefix, prefix);
            assert.strictEqual(list.firstCall.args[1].maxKeys, 10);
            assert.strictEqual('marker' in list.firstCall.args[1], false);
            assert.strictEqual(list.secondCall.args[1].marker, `${prefix}one`);
        });

        it('should pass any other metadata error through', async () => {
            sinon.stub(recordStore.getClient(), 'listObject').callsArgWith(3, errors.InternalError);
            const [err] = await call(recordStore.listDestinations, accountId, { maxKeys: 10 }, log);
            assert.strictEqual(err?.message, 'InternalError');
        });
    });

    describe('start and state', () => {
        it('should be starting before start', () => {
            assert.strictEqual(recordStore.state(), 'starting');
        });

        it('should set the client up before it reads the marker', async () => {
            const client = recordStore.getClient();
            const setup = sinon.spy(client, 'setup');
            const get = sinon.spy(client, 'getObjectMD');
            const [err] = await call(recordStore.start, log);
            assert.ifError(err);
            assert.strictEqual(setup.calledOnce, true);
            assert.strictEqual(setup.calledBefore(get), true);
            assert.strictEqual(recordStore.state(), 'ready');
        });

        it('should not read the marker until the client setup calls back', async () => {
            const client = recordStore.getClient();
            const setup = sinon.stub(client, 'setup');
            const get = sinon.spy(client, 'getObjectMD');
            const done = new Promise(resolve => recordStore.start(log, resolve));
            await new Promise(resolve => setImmediate(resolve));
            assert.strictEqual(get.called, false);
            assert.strictEqual(recordStore.state(), 'starting');
            setup.yield();
            await done;
            assert.strictEqual(get.called, true);
            assert.strictEqual(recordStore.state(), 'ready');
        });

        it('should build a new client after stop', () => {
            const first = recordStore.getClient();
            recordStore.stop();
            assert.notStrictEqual(recordStore.getClient(), first);
        });

        it('should not read the marker when stopped during the client setup', async () => {
            const client = recordStore.getClient();
            const setup = sinon.stub(client, 'setup');
            // stop() drops the client, so watch the reads of every client
            const get = sinon.spy(MetadataWrapper.prototype, 'getObjectMD');
            recordStore.start(log);
            recordStore.stop();
            setup.yield();
            await new Promise(resolve => setImmediate(resolve));
            assert.strictEqual(get.called, false);
            assert.strictEqual(recordStore.state(), 'starting');
        });

        it('should write the marker and be ready when the records bucket is missing', async () => {
            const [err] = await call(recordStore.start, log);
            assert.ifError(err);
            assert.strictEqual(recordStore.state(), 'ready');
            assert.deepStrictEqual(metadata.keyMaps.get(bucket).get(markerKey), { keyFormat: 1 });
        });

        it('should write the marker and be ready when the marker is missing', async () => {
            await call(recordStore.putDestination, accountId, 'orders', makeRecord(accountId, 'orders'), log);
            const [err] = await call(recordStore.start, log);
            assert.ifError(err);
            assert.strictEqual(recordStore.state(), 'ready');
            assert.deepStrictEqual(metadata.keyMaps.get(bucket).get(markerKey), { keyFormat: 1 });
        });

        it('should be ready without writing when the marker holds format 1', async () => {
            await call(recordStore.start, log);
            recordStore.stop();
            const put = sinon.spy(recordStore.getClient(), 'putObjectMD');
            const [err] = await call(recordStore.start, log);
            assert.ifError(err);
            assert.strictEqual(recordStore.state(), 'ready');
            assert.strictEqual(put.called, false);
        });

        it('should be unknownFormat when the marker holds another format', async () => {
            await call(recordStore.start, log);
            recordStore.stop();
            metadata.keyMaps.get(bucket).set(markerKey, { keyFormat: 2 });
            const get = sinon.spy(recordStore.getClient(), 'getObjectMD');
            const [err] = await call(recordStore.start, log);
            assert.ifError(err);
            assert.strictEqual(recordStore.state(), 'unknownFormat');
            assert.strictEqual(get.callCount, 1);
            assert.deepStrictEqual(metadata.keyMaps.get(bucket).get(markerKey), { keyFormat: 2 });
        });

        it('should settle on the marker that another instance wrote in between', async () => {
            const client = recordStore.getClient();
            const put = sinon.stub(client, 'putObjectMD');
            put.callsFake((...args) => {
                const cb = args[5];
                put.wrappedMethod.call(client, ...args.slice(0, 5), err => {
                    if (!err) {
                        metadata.keyMaps.get(bucket).set(markerKey, { keyFormat: 2 });
                    }
                    cb(err);
                });
            });
            const [err] = await call(recordStore.start, log);
            assert.ifError(err);
            assert.strictEqual(recordStore.state(), 'unknownFormat');
        });

        it('should be unknownFormat when the marker holds no format', async () => {
            await call(recordStore.start, log);
            recordStore.stop();
            metadata.keyMaps.get(bucket).set(markerKey, {});
            await call(recordStore.start, log);
            assert.strictEqual(recordStore.state(), 'unknownFormat');
        });

        describe('on a transport error', () => {
            let clock;

            beforeEach(() => {
                clock = sinon.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
                // With Math.random at 0, the jitter adds nothing. Thus the
                // delays are exact. The afterEach of the store restores it.
                sinon.stub(Math, 'random').returns(0);
            });

            afterEach(() => {
                clock.restore();
            });

            it('should stay starting and retry from 1s, doubling up to 30s', async () => {
                // The first start writes the marker. Thus the last read finds
                // a marker and settles the store with no write.
                await call(recordStore.start, log);
                recordStore.stop();
                const get = sinon.stub(recordStore.getClient(), 'getObjectMD');
                const failures = 7;
                for (let i = 0; i < failures; i++) {
                    get.onCall(i).callsArgWith(4, errors.InternalError);
                }
                get.callThrough();
                const done = sinon.spy();
                recordStore.start(log, done);
                await clock.tickAsync(0);
                assert.strictEqual(get.callCount, 1);
                assert.strictEqual(recordStore.state(), 'starting');

                const delays = [1000, 2000, 4000, 8000, 16000, 30000, 30000];
                for (let i = 0; i < delays.length; i++) {
                    await clock.tickAsync(delays[i] - 1);
                    assert.strictEqual(get.callCount, i + 1, `no retry before ${delays[i]} ms`);
                    await clock.tickAsync(1);
                    assert.strictEqual(get.callCount, i + 2, `a retry after ${delays[i]} ms`);
                }
                assert.strictEqual(recordStore.state(), 'ready');
                assert.strictEqual(done.calledOnce, true);
            });

            it('should spread a retry delay by at most 10%', async () => {
                // An odd first decimal adds the jitter: 1000 + 0.95 * 0.1 * 1000
                Math.random.returns(0.95);
                // The first start writes the marker. Thus the retry reads it
                // once and settles the store.
                await call(recordStore.start, log);
                recordStore.stop();
                const get = sinon.stub(recordStore.getClient(), 'getObjectMD');
                get.onFirstCall().callsArgWith(4, errors.InternalError);
                get.callThrough();
                recordStore.start(log);
                await clock.tickAsync(0);
                await clock.tickAsync(1094);
                assert.strictEqual(get.callCount, 1, 'no retry before 1095 ms');
                await clock.tickAsync(1);
                assert.strictEqual(get.callCount, 2, 'a retry after 1095 ms');
            });

            it('should start again from 1s after a stop', async () => {
                const get = sinon.stub(MetadataWrapper.prototype, 'getObjectMD').callsArgWith(4, errors.InternalError);
                recordStore.start(log);
                await clock.tickAsync(0);
                await clock.tickAsync(1000 + 2000);
                assert.strictEqual(get.callCount, 3);
                recordStore.stop();
                recordStore.start(log);
                await clock.tickAsync(0);
                assert.strictEqual(get.callCount, 4);
                await clock.tickAsync(1000);
                assert.strictEqual(get.callCount, 5, 'a retry after 1s');
            });

            it('should retry when the marker write fails', async () => {
                const put = sinon.stub(recordStore.getClient(), 'putObjectMD');
                put.onFirstCall().callsArgWith(5, errors.InternalError);
                put.callThrough();
                recordStore.start(log);
                await clock.tickAsync(0);
                assert.strictEqual(recordStore.state(), 'starting');
                await clock.tickAsync(1000);
                assert.strictEqual(recordStore.state(), 'ready');
                assert.deepStrictEqual(metadata.keyMaps.get(bucket).get(markerKey), { keyFormat: 1 });
            });

            [
                ['throws', setup => setup.onFirstCall().throws(new Error('cannot open the database'))],
                ['calls back with an error', setup => setup.onFirstCall().callsArgWith(0, errors.InternalError)],
            ].forEach(([desc, fail]) => {
                it(`should warn and retry the client setup when it ${desc}`, async () => {
                    const client = recordStore.getClient();
                    const setup = sinon.stub(client, 'setup');
                    fail(setup);
                    setup.callThrough();
                    const get = sinon.spy(client, 'getObjectMD');
                    const startLog = new DummyRequestLogger();
                    assert.doesNotThrow(() => recordStore.start(startLog));
                    await clock.tickAsync(0);
                    assert.strictEqual(recordStore.state(), 'starting');
                    assert.strictEqual(get.called, false);
                    assert.strictEqual(startLog.counts.warn, 1);
                    await clock.tickAsync(999);
                    assert.strictEqual(setup.callCount, 1, 'no retry before 1s');
                    await clock.tickAsync(1);
                    assert.strictEqual(setup.callCount, 2);
                    assert.strictEqual(recordStore.state(), 'ready');
                });
            });

            it('should not set the client up again when the step after a synchronous setup throws', async () => {
                const client = recordStore.getClient();
                const setup = sinon.stub(client, 'setup').callsArg(0);
                // stop() drops the client, so watch the reads of every client
                const get = sinon.stub(MetadataWrapper.prototype, 'getObjectMD').throws(new Error('read failed'));
                const startLog = new DummyRequestLogger();
                assert.throws(() => recordStore.start(startLog), /read failed/);
                await clock.tickAsync(60000);
                assert.strictEqual(setup.callCount, 1);
                assert.strictEqual(get.callCount, 1);
                assert.strictEqual(startLog.counts.warn, 0);
            });

            it('should stop retrying once stopped', async () => {
                // stop() drops the client, so watch the reads of every client
                const get = sinon.stub(MetadataWrapper.prototype, 'getObjectMD').callsArgWith(4, errors.InternalError);
                recordStore.start(log);
                await clock.tickAsync(0);
                recordStore.stop();
                await clock.tickAsync(60000);
                assert.strictEqual(get.callCount, 1);
                assert.strictEqual(recordStore.state(), 'starting');
            });
        });
    });
});
