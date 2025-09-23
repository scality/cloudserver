/* eslint-disable no-console */
const { DummyRequestLogger } = require('../../unit/helpers');
const assert = require('assert');
const log = new DummyRequestLogger();
const helpers = require('./helpers');
const { spawn } = require('child_process');
const path = require('path');
const kms = require('../../../lib/kms/wrapper');
const { promisify } = require('util');

const BUCKET_NUMBER = 10;
const OBJECT_NUMBER = 200;
const TOTAL_OBJECTS = BUCKET_NUMBER * OBJECT_NUMBER;

const KMS_NODES = helpers.config.kmip.transport.length;
const TOTAL_OBJECTS_PER_NODE = Math.floor(TOTAL_OBJECTS / KMS_NODES);

/**
 * 20% approximation for the number of packets per IP
 * As we might not have an exact match of packets and the
 * round robin is confined to each nodejs cluster processes
 * Increased tolerance for AWS SDK v3 connection pooling behavior
 */
const APPROX = Math.floor(0.20 * TOTAL_OBJECTS_PER_NODE);
const EXPECTED_MIN = TOTAL_OBJECTS_PER_NODE - APPROX;
const EXPECTED_MAX = TOTAL_OBJECTS_PER_NODE + APPROX;

async function spawnTcpdump(port, packetCount) {
    const scriptPath = path.join(__dirname, 'countPacketsByIp.sh');
    return new Promise((resolve, reject) => {
        const child = spawn(
            'sudo', // run as root to allow tcpdump execution
            // 5m timeout as process is detached
            ['timeout', 300, scriptPath, port, packetCount],
            {
                // detach to not mess with the tty, it would cause issues with
                // \r even when using another shell and piping stdout.
                detached: true,
                stdio: ['ignore', 'pipe', 'pipe'], // ignored stdin
                shell: false, // no need as it's detached

            },
        );
        let stderr = '';
        child.stderr.on('data', data => {
            stderr += data.toString();
        });

        /** Let a small 10ms timeout for a potential error */
        let spawnTimeout;
        child.on('spawn', () => {
            spawnTimeout = setTimeout(() => {
                if (child.exitCode !== null || child.signalCode !== null) {
                    const err = `countPacketsByIp.sh stopped after spawn with code ${
                        child.exitCode} and signal ${child.signalCode}.\nStderr: ${stderr}`;
                    reject(new Error(err));
                } else {
                    resolve(child);
                }
            }, 10);
        });
        child.on('error', err => {
            if (spawnTimeout) {
                clearTimeout(spawnTimeout);
            }
            reject(new Error(`${err.toString()}\nStderr: ${stderr}`));
        });

        child.on('close', (code, signal) => {
            if (code) {
                if (spawnTimeout) {
                    clearTimeout(spawnTimeout);
                }
                reject(new Error(
                    `tcpdump script closed with code ${code} and signal ${signal}.\nStderr: ${stderr}`
                ));
            }
        });
    });
}

async function stopTcpdump(tcpdump) {
    if (tcpdump.exitCode !== null || tcpdump.signalCode !== null) {
        // tcpdump already closed, no need to kill it
        return;
    }
    await new Promise(resolve => {
        tcpdump.on('close', resolve);
        tcpdump.kill('SIGTERM');
    });
}

describe(`KMS load (kmip cluster ${KMS_NODES} nodes): ${OBJECT_NUMBER
    } objs each in ${BUCKET_NUMBER} bkts (${TOTAL_OBJECTS} objs)`, () => {
    let buckets = [];
    let tcpdumpProcess;
    let stdout;
    let stderr;
    let closePromise;

    before(async () => {
        buckets = await Promise.all(
            new Array(BUCKET_NUMBER).fill(0).map(async (_, i) => {
                const Bucket = `kms-load-${i}`;
                const { masterKeyArn } = await helpers.createKmsKey(log);

                await helpers.s3.createBucket({ Bucket });
                await helpers.s3.putBucketEncryption({
                    Bucket,
                    ServerSideEncryptionConfiguration: helpers.hydrateSSEConfig({
                        algo: 'aws:kms', masterKeyId: masterKeyArn }),
                });

                return { Bucket, masterKeyArn };
            }));
    });

    after(async () => {
        await Promise.all(buckets.map(async ({ Bucket, masterKeyArn }) => {
            await helpers.cleanup(Bucket);
            return helpers.destroyKmsKey(masterKeyArn, log);
        }));
        await promisify(kms.client.stop.bind(kms.client))();
    });

    beforeEach(async () => {
        // tcpdump can catch more than TOTAL_OBJECTS packets because there are PSH and ACK packets
        // but we need to ensure it actually stops before there is no more packets
        // to count packets by IP
        tcpdumpProcess = await spawnTcpdump(5696, TOTAL_OBJECTS);
        stdout = '';
        stderr = '';
        tcpdumpProcess.stderr.on('data', data => {
            stderr += data.toString();
        });
        tcpdumpProcess.stdout.on('data', data => {
            stdout += data.toString();
        });
        closePromise = new Promise(resolve => {
            tcpdumpProcess.on('close', (code, signal) =>
                resolve({
                    code,
                    signal,
                    repartition: stdout
                        .split('\n')
                        .filter(l => l)
                        .map(line => {
                            const [count, ip] = line.trim().split(' ');
                            return { count: +count, ip };
                        }),
                })
            );
        });
    });

    afterEach(async () => {
        if (tcpdumpProcess) {
            await stopTcpdump(tcpdumpProcess);
        }
    });

    async function assertRepartition(closePromise) {
        const { code, signal, repartition } = await closePromise;
        console.log('Test Details', {
            KMS_NODES,
            TOTAL_OBJECTS,
            TOTAL_OBJECTS_PER_NODE,
            APPROX,
            EXPECTED_MIN,
            EXPECTED_MAX,
            code,
            signal,
            stderr,
            repartition,
        });
        const repartitionCount = repartition.map(({ count }) => count);
        assert.strictEqual(code, 0, `tcpdump script closed with code ${code} and signal ${signal}`);
        assert(repartition.length === KMS_NODES, `Expected ${KMS_NODES} IPs but got ${repartition.length}`);
        assert(repartitionCount.every(count =>
            count >= EXPECTED_MIN && count <= EXPECTED_MAX),
            `Repartition counts should be around ${TOTAL_OBJECTS_PER_NODE} but got ${repartitionCount}`);
    }

    it(`should encrypt ${TOTAL_OBJECTS} times in parallel, ~${TOTAL_OBJECTS_PER_NODE} per node`, async () => {
        await (Promise.all(
            buckets.map(async ({ Bucket }) => Promise.all(
                new Array(OBJECT_NUMBER).fill(0).map(async (_, i) =>
                    helpers.s3.putObject({ Bucket, Key: `obj-${i}`, Body: `body-${i}` }))
            ))
        ));
        await assertRepartition(closePromise);
    });

    it(`should decrypt ${TOTAL_OBJECTS} times in parallel, ~${TOTAL_OBJECTS_PER_NODE} per node`, async () => {
        // First verify all objects exist
        await Promise.all(
            buckets.map(async ({ Bucket }) => Promise.all(
                new Array(OBJECT_NUMBER).fill(0).map(async (_, i) => {
                    try {
                        await helpers.s3.headObject({ Bucket, Key: `obj-${i}` });
                    } catch (err) {
                        if (err.code === 'NoSuchKey') {
                            // If object doesn't exist, create it
                            await helpers.s3.putObject({ 
                                Bucket, 
                                Key: `obj-${i}`, 
                                Body: `body-${i}` 
                            });
                        } else {
                            throw err;
                        }
                    }
                })
            ))
        );

        // Now perform the parallel decryption test
        await Promise.all(
            buckets.map(async ({ Bucket }) => Promise.all(
                new Array(OBJECT_NUMBER).fill(0).map(async (_, i) =>
                    helpers.s3.getObject({ Bucket, Key: `obj-${i}` }))
            ))
        );
        await assertRepartition(closePromise);
    });
});
