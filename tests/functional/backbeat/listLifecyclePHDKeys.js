const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  HeadObjectCommand,
  PutBucketVersioningCommand,
  PutObjectCommand,
} = require('@aws-sdk/client-s3');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const {
  removeAllVersions,
} = require('../aws-node-sdk/lib/utility/versioning-util');
const { makeBackbeatRequest } = require('./utils');
const { promisify } = require('util');

/**
 * Deleting an object's current version writes a PHD master.
 * If no other version survives, it stays dangling. A run of dangling
 * PHDs longer than max-scanned-lifecycle-listing-entries is a desert
 * ISSUE: it used to truncate the orphan and noncurrent listings with no resume marker, so
 * backbeat requeued the same listing forever.
 *
 * Metadata repairs a deleted key's PHD master after 15s (arsenal
 * VersioningRequestProcessor.processVersionSpecificDelete). assertDesertWasScanned()
 * checks the resume marker landed inside the desert, proving the scan cap hit
 * before repair could shrink it. Seeding takes ~200ms, well inside that window.
 *
 * v1: skipped for bucketd/file -- its v1 format never generates a PHD master
 * (arsenal delimiterVersions.js: "S3C does not use PHDs in V1 format"). 
 * NOTE: Mongo v1 does use PHDs but mongo is already skipped below for the dangling case.
 * mongo: a zero-survivor PHD master is deleted synchronously, in the same DELETE request
 * (MongoClientInterface.deleteOrRepairPHD) so no window for it to persist, so
 * this suite can not seed a desert there.
 * bucketd/file repair it on a best-effort 15s timer instead, which is the window that lets it go dangling.
 */
const isV1 = process.env.DEFAULT_BUCKET_KEY_FORMAT === 'v1';
const isMongo = process.env.S3METADATA === 'mongodb';
const describePHD = isV1 || isMongo ? describe.skip : describe;

const bucketUtil = new BucketUtility('default', {});
const s3 = bucketUtil.s3;

const removeAllVersionsPromise = promisify(removeAllVersions);

const DESERT_SIZE = 12;
const DESERT_PREFIX = 'phd-';
const SEED_CONCURRENCY = 8;
const SCAN_CAP = '5';
// Hard page guard. A markerless listing that keeps restarting fails fast, and does not hang.
const MAX_PAGES = 20;
// Metadata repair delay, per key. See "Timing contract" above.
const PHD_REPAIR_WINDOW_MS = 15000;

let credentials = null;

async function getCredentials() {
  const creds = await s3.config.credentials();
  return {
    accessKey: creds.accessKeyId,
    secretKey: creds.secretAccessKey,
  };
}

function uniqueBucket(prefix) {
  return `${prefix}-${crypto.randomBytes(4).toString('hex')}`;
}

function desertKey(n) {
  return `${DESERT_PREFIX}${`00${n}`.slice(-3)}`;
}

function putObject(bucket, key, cb) {
  return s3
    .send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: '123' }))
    .then(data => cb(null, data.VersionId))
    .catch(cb);
}

function deleteVersion(bucket, key, versionId, cb) {
  return s3
    .send(
      new DeleteObjectCommand({
        Bucket: bucket,
        Key: key,
        VersionId: versionId,
      }),
    )
    .then(() => cb())
    .catch(cb);
}

/**
 * Creates a dangling PHD master. Put one version, then delete that exact version.
 * Metadata replaces the master with { isPHD: true } and starts its 15s repair.
 * Until the repair runs, or a GET or HEAD triggers it, the key is a zero-version
 * PHD master. These keys are the desert material for the tests below.
 */
function createDanglingPHD(bucket, key, cb) {
  return putObject(bucket, key, (err, versionId) =>
    err ? cb(err) : deleteVersion(bucket, key, versionId, cb),
  );
}

/**
 * Seeds DESERT_SIZE dangling PHD masters. Returns the time seeding started. That
 * time bounds the earliest repair deadline, because each key's own delete happens
 * later. The bound is therefore safe.
 */
function seedDesert(bucket, cb) {
  const seededAt = Date.now();
  return async.timesLimit(
    DESERT_SIZE,
    SEED_CONCURRENCY,
    (n, next) => createDanglingPHD(bucket, desertKey(n), next),
    err => cb(err, seededAt),
  );
}

/**
 * HEADs every desert key. A GET or HEAD on a PHD master triggers the metadata
 * repair, which deletes a zero-version master. Every key returns 404. The code
 * ignores errors on purpose. This clears the desert at once, instead of waiting
 * for the repair timers. It also matters more than tidiness, because PHD masters
 * outlive their bucket.
 */
function repairDesert(bucket, cb) {
  return async.timesLimit(
    DESERT_SIZE,
    SEED_CONCURRENCY,
    (n, next) =>
      s3
        .send(new HeadObjectCommand({ Bucket: bucket, Key: desertKey(n) }))
        .then(() => next())
        .catch(() => next()),
    cb,
  );
}

function createOrphanDeleteMarker(bucket, key, cb) {
  return putObject(bucket, key, (err, versionId) => {
    if (err) {
      return cb(err);
    }
    return s3
      .send(new DeleteObjectCommand({ Bucket: bucket, Key: key }))
      .then(() => deleteVersion(bucket, key, versionId, cb))
      .catch(cb);
  });
}

function createVersionedBucket(bucket, cb) {
  return s3
    .send(new CreateBucketCommand({ Bucket: bucket }))
    .then(() =>
      s3.send(
        new PutBucketVersioningCommand({
          Bucket: bucket,
          VersioningConfiguration: { Status: 'Enabled' },
        }),
      ),
    )
    .then(() => cb())
    .catch(cb);
}

function cleanupBucket(bucket, cb) {
  return async.series(
    [
      next => repairDesert(bucket, next),
      next =>
        removeAllVersionsPromise({ Bucket: bucket })
          .then(() => next())
          .catch(next),
      next =>
        s3
          .send(new DeleteBucketCommand({ Bucket: bucket }))
          .then(() => next())
          .catch(next),
    ],
    cb,
  );
}

/**
 * Reads every page of a lifecycle listing. Feeds each returned marker back into
 * the next request. Checks the core invariant on every page: a truncated page
 * must return a resume marker, and that marker must move forward. This is the
 * regression itself -- before the fix, a desert truncated with no marker at all.
 */
function listAllPages(params, cb) {
  const { bucket, listType, scanCap } = params;
  const pages = [];
  let keyMarker;
  let versionIdMarker;
  let done = false;

  return async.whilst(
    () => !done,
    next => {
      const queryObj = {
        'list-type': listType,
        'max-scanned-lifecycle-listing-entries': scanCap,
      };
      if (keyMarker !== undefined) {
        if (listType === 'orphan') {
          queryObj.marker = keyMarker;
        } else {
          queryObj['key-marker'] = keyMarker;
          if (versionIdMarker !== undefined) {
            queryObj['version-id-marker'] = versionIdMarker;
          }
        }
      }
      return makeBackbeatRequest(
        {
          method: 'GET',
          bucket,
          queryObj,
          authCredentials: credentials,
        },
        (err, response) => {
          if (err) {
            return next(err);
          }
          if (response.statusCode !== 200) {
            return next(
              new Error(
                `${listType} listing returned ${response.statusCode}: ` +
                  `${String(response.body).slice(0, 200)}`,
              ),
            );
          }
          const data = JSON.parse(response.body);
          pages.push(data);

          if (pages.length > MAX_PAGES) {
            return next(
              new Error(
                `listing did not terminate within ${MAX_PAGES} pages: ` +
                  'markerless truncation restarts it from scratch',
              ),
            );
          }

          if (!data.IsTruncated) {
            done = true;
            return next();
          }

          // Report invariant violations through the callback, do not throw them. An
          // assertion thrown in this HTTP callback becomes an uncaught exception, and
          // mocha can blame another test for it.
          const nextKeyMarker =
            listType === 'orphan' ? data.NextMarker : data.NextKeyMarker;
          // The core invariant. A truncated listing must return a resume marker,
          if (!nextKeyMarker) {
            return next(
              new Error(
                `truncated ${listType} listing page ${pages.length} returned no marker ` +
                  `(request marker: ${keyMarker || '<none>'})`,
              ),
            );
          }
          // and that marker must move forward on every page.
          if (keyMarker !== undefined) {
            if (nextKeyMarker < keyMarker) {
              return next(
                new Error(
                  `marker went backwards: ${keyMarker} -> ${nextKeyMarker}`,
                ),
              );
            }
            const prevTuple = `${keyMarker}\0${versionIdMarker || ''}`;
            const newTuple = `${nextKeyMarker}\0${data.NextVersionIdMarker || ''}`;
            if (newTuple === prevTuple) {
              return next(
                new Error(
                  'marker did not advance on truncated ' +
                    `${listType} page ${pages.length}: ${nextKeyMarker}`,
                ),
              );
            }
          }
          keyMarker = nextKeyMarker;
          versionIdMarker = data.NextVersionIdMarker;
          return next();
        },
      );
    },
    err => (err ? cb(err) : cb(null, pages)),
  );
}

const seedDesertPromise = promisify(seedDesert);
const listAllPagesPromise = promisify(listAllPages);

/** Seeds a fresh desert, then reads every page of a capped listing across it. */
async function seedThenList(bucket, listType, scanCap) {
  const seededAt = await seedDesertPromise(bucket);
  const pages = await listAllPagesPromise({ bucket, listType, scanCap });
  return { pages, seededAt };
}

/** Proves the scan cap ran out inside the desert, so the assertions that follow mean something. */
function assertDesertWasScanned(pages, seededAt, label) {
  const markers = pages
    .map(page => page.NextMarker || page.NextKeyMarker)
    .filter(Boolean);
  if (markers.some(marker => marker.startsWith(DESERT_PREFIX))) {
    return;
  }
  const elapsed = Date.now() - seededAt;
  const cause =
    elapsed >= PHD_REPAIR_WINDOW_MS
      ? `seeding+listing took ${elapsed}ms, over the ${PHD_REPAIR_WINDOW_MS}ms metadata repair ` +
        'window: the desert was repaired before the listing ran (slow runner, not a code bug)'
      : `only ${elapsed}ms elapsed, well inside the ${PHD_REPAIR_WINDOW_MS}ms repair window: the ` +
        'desert was never created, so this backend did not write PHD masters (v0 buckets only)';
  assert.fail(
    `${label}: no resume marker landed inside the desert ` +
      `(markers: ${JSON.stringify(markers)}) -- ${cause}`,
  );
}

function contentsKeys(pages) {
  return pages.reduce(
    (acc, page) => acc.concat((page.Contents || []).map(entry => entry.Key)),
    [],
  );
}

function contentsEntries(pages) {
  return pages.reduce((acc, page) => acc.concat(page.Contents || []), []);
}

describePHD('listLifecycle over a dangling-PHD desert', () => {
  before(async () => {
    credentials = await getCredentials();
  });

  // scanCap = 5, desert = 12 dangling PHDs (phd-001..012) in both buckets below.

  describe('orphan crosses the desert', () => {
    const bucket = uniqueBucket('lc-phd-orphan');

    before(done =>
      async.series(
        [
          next => createVersionedBucket(bucket, next),
          // held as a candidate up to the desert
          next => createOrphanDeleteMarker(bucket, 'aaa-dm', next),
          // past the desert
          next => createOrphanDeleteMarker(bucket, 'zzz-dm', next),
        ],
        done,
      ),
    );

    after(done => cleanupBucket(bucket, done));

    it('should list both orphan delete markers across the desert', async () => {
      const { pages, seededAt } = await seedThenList(bucket, 'orphan', SCAN_CAP);
      assertDesertWasScanned(pages, seededAt, 'orphan');
      assert.deepStrictEqual(contentsKeys(pages), ['aaa-dm', 'zzz-dm']);
    });
  });

  describe('noncurrent crosses the desert', () => {
    const bucket = uniqueBucket('lc-phd-noncurrent');
    const aaaNcVersionIds = [];
    const survivorVersionIds = [];
    const zzzNcVersionIds = [];

    before(done =>
      async.series(
        [
          next => createVersionedBucket(bucket, next),
          // 2 versions before the desert, older one is noncurrent
          next =>
            async.timesSeries(
              2,
              (n, cb) =>
                putObject(bucket, 'aaa-nc', (err, versionId) => {
                  aaaNcVersionIds.push(versionId);
                  cb(err);
                }),
              next,
            ),
          // 3 puts past the desert, newest deleted by id -> PHD master with 2
          // survivors: oldest is noncurrent, newest must stay protected
          next =>
            async.timesSeries(
              3,
              (n, cb) =>
                putObject(bucket, 'www-survivors', (err, versionId) => {
                  survivorVersionIds.push(versionId);
                  cb(err);
                }),
              next,
            ),
          next =>
            deleteVersion(
              bucket,
              'www-survivors',
              survivorVersionIds[2],
              next,
            ),
          // 2 more versions past the desert, older one is noncurrent
          next =>
            async.timesSeries(
              2,
              (n, cb) =>
                putObject(bucket, 'zzz-nc', (err, versionId) => {
                  zzzNcVersionIds.push(versionId);
                  cb(err);
                }),
              next,
            ),
        ],
        done,
      ),
    );

    after(done => cleanupBucket(bucket, done));

    it('should list noncurrent versions on both sides of the desert and protect the PHD survivor', async () => {
      const { pages, seededAt } = await seedThenList(bucket, 'noncurrent', SCAN_CAP);
      assertDesertWasScanned(pages, seededAt, 'noncurrent');
      const listed = contentsEntries(pages);
      assert.deepStrictEqual(
        listed.map(entry => `${entry.Key}:${entry.VersionId}`).sort(),
        [
          `aaa-nc:${aaaNcVersionIds[0]}`,
          `www-survivors:${survivorVersionIds[0]}`,
          `zzz-nc:${zzzNcVersionIds[0]}`,
        ],
      );
      const survivorVersions = listed
        .filter(entry => entry.Key === 'www-survivors')
        .map(entry => entry.VersionId);
      assert(
        !survivorVersions.includes(survivorVersionIds[1]),
        'newest surviving version under the PHD master listed as noncurrent: ' +
          'NCVE would expire live data',
      );
    });
  });
});
