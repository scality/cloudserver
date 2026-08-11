const assert = require("assert");
const async = require("async");
const crypto = require("crypto");
const {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  HeadObjectCommand,
  ListObjectVersionsCommand,
  PutBucketVersioningCommand,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");
const BucketUtility = require("../aws-node-sdk/lib/utility/bucket-util");
const {
  removeAllVersions,
} = require("../aws-node-sdk/lib/utility/versioning-util");
const { makeBackbeatRequest } = require("./utils");
const { promisify } = require("util");

/**
 * Tests for lifecycle listings over PHD master keys.
 *
 * Metadata writes a PHD master when you delete the current version of an object
 * by version id. A run of PHD masters longer than
 * max-scanned-lifecycle-listing-entries is a "desert". A desert used to truncate
 * the orphan and noncurrent lifecycle listings with no resume marker, so backbeat
 * requeued the same listing from scratch forever.
 *
 * Deserts last on the backends this suite runs on. They are not a passing state.
 * PHD repair is best-effort and lives in memory, so a metadata restart loses it.
 * Nothing repairs a key that no client reads again. PHD masters therefore build up
 * and can stay in the keyspace.
 *
 * Timing contract
 * ---------------
 * These tests still race that repair, because a single-process metadata backend
 * does run it. Metadata starts a 15s repair timer for each key when you delete
 * that key's version (arsenal
 * VersioningRequestProcessor.processVersionSpecificDelete). When the timer fires,
 * metadata deletes the zero-version PHD master and the desert shrinks.
 *
 * One assertion depends on that window: assertDesertWasScanned(). It requires a
 * resume marker on a desert key. That proves the scan cap ran out inside the
 * desert, so the listing did use the PHD path. Seeding takes about 200ms against
 * a 15s deadline, so the margin is wide. If a run ever passes that deadline, the
 * failure message says so, and does not blame metadata.
 *
 * The other assertions do not depend on the timer. A repair during a listing only
 * shrinks the desert. It cannot add or remove an expected key, and it can only
 * turn a truncated page into a complete one.
 *
 * The sentinel does not check "page 0 was truncated", on purpose. Any bucket that
 * holds more than scanCap entries of its own truncates page 0, with or without a
 * PHD master. That weaker check passes on a fully repaired desert, and makes the
 * test prove nothing.
 *
 * Bucket names carry a random suffix, also on purpose. PHD master keys outlive
 * DeleteBucket, because DelimiterVersions hides them from the bucket-emptiness
 * check. With a fixed name, a run that dies before its after() hook poisons the
 * next run on the same environment.
 *
 * Backends that cannot build a desert
 * -----------------------------------
 * The bug needs a run of dangling PHD masters, meaning zero-version ones. Only
 * those consume scan budget without moving the marker. A PHD master that still
 * has versions is not desert material: in v0 its version keys sort immediately
 * after it, and each one sets the marker.
 *
 * v1 buckets: PHD masters exist only in v0, so there is nothing to test.
 *
 * mongo: a zero-version PHD master dies inside its own DELETE.
 * MongoClientInterface.deleteOrRepairPHD removes it before the request answers.
 * So the S3 API cannot seed a desert. bucketd and file keep the master and
 * repair it 15s later, which is why the seeding works there.
 *
 * Skipping mongo loses no coverage. handlePHDMaster only reads the v0 key
 * stream and writes the marker, the same on every backend. file-ft-tests and
 * s3c-ft-tests-v0 run this suite.
 */
const isV1 = process.env.DEFAULT_BUCKET_KEY_FORMAT === "v1";
const isMongo = process.env.S3METADATA === "mongodb";
const describePHD = isV1 || isMongo ? describe.skip : describe;

const bucketUtil = new BucketUtility("default", {});
const s3 = bucketUtil.s3;

const removeAllVersionsPromise = promisify(removeAllVersions);

const DESERT_SIZE = 12;
const DESERT_PREFIX = "phd-";
const SEED_CONCURRENCY = 8;
const SCAN_CAP = "5";
const SMALL_SCAN_CAP = "3";
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
  return `${prefix}-${crypto.randomBytes(4).toString("hex")}`;
}

function desertKey(n) {
  return `${DESERT_PREFIX}${`00${n}`.slice(-3)}`;
}

function putObject(bucket, key, cb) {
  return s3
    .send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: "123" }))
    .then((data) => cb(null, data.VersionId))
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
    (err) => cb(err, seededAt),
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
          VersioningConfiguration: { Status: "Enabled" },
        }),
      ),
    )
    .then(() => cb())
    .catch(cb);
}

function cleanupBucket(bucket, cb) {
  return async.series(
    [
      (next) => repairDesert(bucket, next),
      (next) =>
        removeAllVersionsPromise({ Bucket: bucket })
          .then(() => next())
          .catch(next),
      (next) =>
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
 * must return a resume marker, and that marker must move forward.
 */
function listAllPages(params, cb) {
  const { bucket, listType, scanCap } = params;
  const pages = [];
  let keyMarker;
  let versionIdMarker;
  let done = false;

  return async.whilst(
    () => !done,
    (next) => {
      const queryObj = {
        "list-type": listType,
        "max-scanned-lifecycle-listing-entries": scanCap,
      };
      if (keyMarker !== undefined) {
        if (listType === "orphan") {
          queryObj.marker = keyMarker;
        } else {
          queryObj["key-marker"] = keyMarker;
          if (versionIdMarker !== undefined) {
            queryObj["version-id-marker"] = versionIdMarker;
          }
        }
      }
      return makeBackbeatRequest(
        {
          method: "GET",
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
                  "markerless truncation restarts it from scratch",
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
            listType === "orphan" ? data.NextMarker : data.NextKeyMarker;
          // The core invariant. A truncated listing must return a resume marker,
          if (!nextKeyMarker) {
            return next(
              new Error(
                `truncated ${listType} listing page ${pages.length} returned no marker ` +
                  `(request marker: ${keyMarker || "<none>"})`,
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
            const prevTuple = `${keyMarker}\0${versionIdMarker || ""}`;
            const newTuple = `${nextKeyMarker}\0${data.NextVersionIdMarker || ""}`;
            if (newTuple === prevTuple) {
              return next(
                new Error(
                  "marker did not advance on truncated " +
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
    (err) => (err ? cb(err) : cb(null, pages)),
  );
}

/** Seeds a fresh desert, then reads every page of a capped listing across it. */
function seedThenList(bucket, listType, scanCap, cb) {
  return seedDesert(bucket, (err, seededAt) => {
    if (err) {
      return cb(err);
    }
    return listAllPages({ bucket, listType, scanCap }, (err, pages) =>
      err ? cb(err) : cb(null, pages, seededAt),
    );
  });
}

/**
 * Proves the listing paged through the desert, so the assertions that follow mean
 * something.
 *
 * It requires at least one resume marker on a desert key. "Page 0 was truncated"
 * does not prove that. Any bucket that holds more than scanCap entries of its own
 * truncates page 0, with or without a PHD master, so that check also passes on a
 * repaired desert. Only a scan cap that runs out inside the desert puts a marker
 * on a desert key, and that is the condition under test.
 */
function assertDesertWasScanned(pages, seededAt, label) {
  const markers = pages
    .map((page) => page.NextMarker || page.NextKeyMarker)
    .filter(Boolean);
  if (markers.some((marker) => marker.startsWith(DESERT_PREFIX))) {
    return;
  }
  const elapsed = Date.now() - seededAt;
  const cause =
    elapsed >= PHD_REPAIR_WINDOW_MS
      ? `seeding+listing took ${elapsed}ms, over the ${PHD_REPAIR_WINDOW_MS}ms metadata repair ` +
        "window: the desert was repaired before the listing ran (slow runner, not a code bug)"
      : `only ${elapsed}ms elapsed, well inside the ${PHD_REPAIR_WINDOW_MS}ms repair window: the ` +
        "desert was never created, so this backend did not write PHD masters (v0 buckets only)";
  assert.fail(
    `${label}: no resume marker landed inside the desert ` +
      `(markers: ${JSON.stringify(markers)}) -- ${cause}`,
  );
}

/**
 * Reports the outcome of a test through done(). Mocha treats an assertion thrown
 * inside an HTTP callback as an uncaught exception, and can blame the wrong test
 * for it. done() keeps each failure on the test that caused it.
 */
function finish(done, err, assertions) {
  if (err) {
    return done(err);
  }
  try {
    assertions();
  } catch (assertionErr) {
    return done(assertionErr);
  }
  return done();
}

function contentsKeys(pages) {
  return pages.reduce(
    (acc, page) => acc.concat((page.Contents || []).map((entry) => entry.Key)),
    [],
  );
}

function contentsEntries(pages) {
  return pages.reduce((acc, page) => acc.concat(page.Contents || []), []);
}

describePHD("listLifecycle over a dangling-PHD desert", () => {
  before((done) => {
    getCredentials()
      .then((creds) => {
        credentials = creds;
        done();
      })
      .catch(done);
  });

  // One layout, read two ways. A regular object and noncurrent versions sit before
  // the desert. More noncurrent versions and an orphan DM sit after it. Both listing
  // types must cross the desert to reach what lies beyond.
  describe("crossing the desert", () => {
    const bucket = uniqueBucket("lc-phd-desert");
    const aaaVersionIds = [];
    const zzzVersionIds = [];

    before((done) =>
      async.series(
        [
          (next) => createVersionedBucket(bucket, next),
          (next) => putObject(bucket, "aaa-obj", next),
          (next) =>
            async.timesSeries(
              2,
              (n, cb) =>
                putObject(bucket, "aaa-nc", (err, versionId) => {
                  aaaVersionIds.push(versionId);
                  cb(err);
                }),
              next,
            ),
          (next) =>
            async.timesSeries(
              2,
              (n, cb) =>
                putObject(bucket, "zzz-nc", (err, versionId) => {
                  zzzVersionIds.push(versionId);
                  cb(err);
                }),
              next,
            ),
          (next) => createOrphanDeleteMarker(bucket, "zzz-dm", next),
        ],
        done,
      ),
    );

    after((done) => cleanupBucket(bucket, done));

    it("should list the orphan delete marker beyond the desert", (done) =>
      seedThenList(bucket, "orphan", SCAN_CAP, (err, pages, seededAt) =>
        finish(done, err, () => {
          assertDesertWasScanned(pages, seededAt, "orphan");
          assert.deepStrictEqual(contentsKeys(pages), ["zzz-dm"]);
        }),
      ));

    it("should list exactly the noncurrent versions on both sides of the desert", (done) =>
      seedThenList(bucket, "noncurrent", SCAN_CAP, (err, pages, seededAt) =>
        finish(done, err, () => {
          assertDesertWasScanned(pages, seededAt, "noncurrent");
          // Only the first, older version of each key is noncurrent.
          assert.deepStrictEqual(
            contentsEntries(pages)
              .map((entry) => `${entry.Key}:${entry.VersionId}`)
              .sort(),
            [`aaa-nc:${aaaVersionIds[0]}`, `zzz-nc:${zzzVersionIds[0]}`],
          );
        }),
      ));
  });

  describe("orphan DM held as candidate right before the desert", () => {
    const bucket = uniqueBucket("lc-phd-candidate");

    before((done) =>
      async.series(
        [
          (next) => createVersionedBucket(bucket, next),
          (next) => createOrphanDeleteMarker(bucket, "aaa-dm", next),
        ],
        done,
      ),
    );

    after((done) => cleanupBucket(bucket, done));

    it("should emit the orphan DM preceding the desert exactly once", (done) =>
      seedThenList(bucket, "orphan", SMALL_SCAN_CAP, (err, pages, seededAt) =>
        finish(done, err, () => {
          assertDesertWasScanned(pages, seededAt, "orphan/held-candidate");
          // The first PHD key proves the held candidate belongs to another key. The
          // listing must emit the DM before the marker moves past it. A fix that only
          // advanced the marker would strand the DM behind it forever.
          assert.deepStrictEqual(contentsKeys(pages), ["aaa-dm"]);
          assert.deepStrictEqual(
            contentsKeys([pages[0]]),
            ["aaa-dm"],
            "orphan DM must be emitted on the page that scanned past it",
          );
        }),
      ));
  });

  describe("PHD master that still has versions", () => {
    const bucket = uniqueBucket("lc-phd-survivors");
    const versionIds = [];

    before((done) =>
      async.series(
        [
          (next) => createVersionedBucket(bucket, next),
          (next) =>
            async.timesSeries(
              3,
              (n, cb) =>
                putObject(bucket, "ccc-phd-versions", (err, versionId) => {
                  versionIds.push(versionId);
                  cb(err);
                }),
              next,
            ),
          // Delete the current version by version id. Metadata writes a PHD master on
          // ccc-phd-versions, with two versions left under it. The newest of those two
          // is the current version in practice. NCVE must never treat it as expirable.
          // If it did, repair would have nothing to promote, and NCVE would delete live
          // data.
          (next) =>
            deleteVersion(bucket, "ccc-phd-versions", versionIds[2], next),
        ],
        done,
      ),
    );

    after((done) => cleanupBucket(bucket, done));

    it("should never list the newest surviving version of a PHD master as noncurrent", (done) =>
      seedThenList(bucket, "noncurrent", SCAN_CAP, (err, pages, seededAt) =>
        finish(done, err, () => {
          assertDesertWasScanned(
            pages,
            seededAt,
            "noncurrent/phd-with-versions",
          );
          const listedForKey = contentsEntries(pages)
            .filter((entry) => entry.Key === "ccc-phd-versions")
            .map((entry) => entry.VersionId);
          // Only the oldest version is noncurrent. The newest one left, index 1, is
          // the current version in practice, so it stays protected.
          assert.deepStrictEqual(listedForKey, [versionIds[0]]);
          assert(
            !listedForKey.includes(versionIds[1]),
            "newest surviving version listed as noncurrent: NCVE would expire live data",
          );
        }),
      ));
  });

  describe("base version listing regression over a desert", () => {
    const bucket = uniqueBucket("lc-phd-base-listing");
    let versionId;

    before((done) =>
      async.series(
        [
          (next) => createVersionedBucket(bucket, next),
          (next) =>
            putObject(bucket, "aaa-obj", (err, vid) => {
              versionId = vid;
              next(err);
            }),
        ],
        done,
      ),
    );

    after((done) => cleanupBucket(bucket, done));

    it("should paginate ListObjectVersions across the desert without listing PHD keys", (done) =>
      async.waterfall(
        [
          (next) => seedDesert(bucket, next),
          // Run one capped lifecycle listing as the sentinel. Without it, a repaired
          // desert would let the version-listing assertions below pass for nothing.
          (seededAt, next) =>
            listAllPages(
              { bucket, listType: "orphan", scanCap: SCAN_CAP },
              (err, pages) => {
                if (err) {
                  return next(err);
                }
                try {
                  assertDesertWasScanned(
                    pages,
                    seededAt,
                    "base-listing sentinel",
                  );
                } catch (assertionErr) {
                  return next(assertionErr);
                }
                return next();
              },
            ),
          (next) => {
            const collected = [];
            let keyMarker;
            let vidMarker;
            let pageCount = 0;
            let finished = false;
            return async.whilst(
              () => !finished,
              (cb) =>
                s3
                  .send(
                    new ListObjectVersionsCommand({
                      Bucket: bucket,
                      MaxKeys: 1,
                      KeyMarker: keyMarker,
                      VersionIdMarker: vidMarker,
                    }),
                  )
                  .then((data) => {
                    pageCount += 1;
                    if (pageCount > MAX_PAGES) {
                      return cb(
                        new Error("ListObjectVersions did not terminate"),
                      );
                    }
                    collected.push(
                      ...(data.Versions || []),
                      ...(data.DeleteMarkers || []),
                    );
                    if (!data.IsTruncated) {
                      finished = true;
                      return cb();
                    }
                    keyMarker = data.NextKeyMarker;
                    vidMarker = data.NextVersionIdMarker;
                    return cb();
                  })
                  .catch(cb),
              (err) => next(err, collected),
            );
          },
        ],
        (err, collected) => {
          finish(done, err, () => {
            // PHD masters are placeholders, not versions. A version listing must never
            // return them.
            assert.deepStrictEqual(
              collected.map((entry) => `${entry.Key}:${entry.VersionId}`),
              [`aaa-obj:${versionId}`],
            );
          });
        },
      ));
  });

  // A PHD master that still has versions, placed so the scan cap runs out on that
  // master key. handlePHDMaster must leave the resume marker behind it. A marker on
  // the key itself gives a key-marker with no version-id-marker, which S3 reads as
  // "resume after every version of that key". The listing then skips that key's
  // expirable noncurrent versions, and NCVE never sees them.
  //
  // The padding is exact. With cap 5, the v0 keyspace below is:
  //   aaa(master) aaa\0v bbb(master) bbb\0v ccc(PHD master) | ccc\0v1 ccc\0v0 ddd...
  // ccc's PHD master is the 5th entry scanned, so the cap runs out on the 6th.
  describe("cap exhausted on a PHD master that still has versions", () => {
    const bucket = uniqueBucket("lc-phd-cap-landing");
    const cccVersionIds = [];

    before((done) =>
      async.series(
        [
          (next) => createVersionedBucket(bucket, next),
          // Two entries each: a master and one version.
          (next) => putObject(bucket, "aaa", next),
          (next) => putObject(bucket, "bbb", next),
          // ccc gets 3 versions. Deleting the newest by id leaves a PHD master and 2
          // versions under it.
          (next) =>
            async.timesSeries(
              3,
              (n, cb) =>
                putObject(bucket, "ccc", (err, versionId) => {
                  cccVersionIds.push(versionId);
                  cb(err);
                }),
              next,
            ),
          (next) => deleteVersion(bucket, "ccc", cccVersionIds[2], next),
          // A key after ccc, so a skipped ccc looks different from an empty listing.
          (next) => putObject(bucket, "ddd", next),
          (next) => putObject(bucket, "ddd", next),
        ],
        done,
      ),
    );

    after((done) => cleanupBucket(bucket, done));

    it("should not skip the versions of a PHD master when the cap lands on it", (done) =>
      listAllPages(
        { bucket, listType: "noncurrent", scanCap: SCAN_CAP },
        (err, pages) =>
          finish(done, err, () => {
            // The cap must have run out on ccc's PHD master, or the test proves nothing.
            assert.strictEqual(
              pages[0].IsTruncated,
              true,
              "first page not truncated: the padding no longer lands the cap on ccc",
            );
            assert.notStrictEqual(
              pages[0].NextKeyMarker,
              "ccc",
              "marker points at the PHD master itself, so ccc's versions are skipped",
            );
            // ccc's oldest version is noncurrent and expirable. The newest one left is
            // the current version in practice, so it stays protected.
            const listedForCcc = contentsEntries(pages)
              .filter((entry) => entry.Key === "ccc")
              .map((entry) => entry.VersionId);
            assert.deepStrictEqual(listedForCcc, [cccVersionIds[0]]);
          }),
      ));
  });
});
