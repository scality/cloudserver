'use strict'; // eslint-disable-line strict

const commander = require('commander');
const config = require('aws-sdk').config;
const S3 = require('aws-sdk').S3;
const crypto = require('crypto');

const stderr = process.stderr;

class S3Blaster {
    constructor() {
        commander.version('0.0.1')
        .option('-P, --port <port>', 'Port number', parseInt)
        .option('-H, --host [host]', 'Host name')
        .option('-N, --n-threads <nThreads>', 'Number of threads', parseInt)
        .option('-n, --n-ops <nOps>', 'Number of operations', parseInt)
        .option('-u, --n-buckets <nBuckets>', 'Number of buckets', parseInt)
        .option('-B, --bucket-prefix [bucketPrefix]', 'Prefix for bucket name')
        .option('-s, --size <size>', 'Size of data', parseInt)
        .parse(process.argv);
        this.host = commander.host || 'localhost';
        this.port = commander.port || 8000;
        this.nThreads = commander.nThreads || 10;
        this.nOps = commander.nOps || 100;
        this.bucketPrefix = commander.bucketPrefix || 'foo';
        this.nBuckets = commander.nBuckets || 1;
        this.size = commander.size || 4096;
        Object.keys(this).forEach(opt => stderr.write(`${opt}=${this[opt]}\n`));
        config.apiVersions = { s3: '2006-03-01' };
        // config.signatureVersion = 'v2';
        config.accessKeyId = 'accessKey1';
        config.secretAccessKey = 'verySecretKey1';
        config.endpoint = `${this.host}:${this.port}`;
        config.sslEnabled = false;
        // config.logger = process.stdout;
        config.s3ForcePathStyle = true;

        this.s3 = new S3();
        this.value = crypto.randomBytes(this.size);
        this.count = 0;
        this.threads = 0;
        this.nSuccesses = 0;
        this.nFailures = 0;
        this.nBytes = 0;
        this.latSum = 0;
        this.latSumSq = 0;
        this.okBucket = 0;
        this.storedKeys = [];
        this.actions = [];
        this.createdBucketsNb = 0;
    }

    setActions(put, get, del) {
        this.actions = [put || false, get || false, del || false];
    }

    createBucket(bucketName, cb) {
        this.s3.createBucket({ Bucket: bucketName }, (err) => {
            if (!err) {
                return cb();
            }
            const code = err.toString().split(':')[0];
            stderr.write(`createBucket: ${code}..`);
            return cb(code === 'BucketAlreadyExists' ? null : code);
        });
    }

    createBuckets(cb) {
        const bucketName = `${this.bucketPrefix}${this.createdBucketsNb}`;
        stderr.write(`creating bucket ${bucketName}..`);
        this.createBucket(bucketName, (err) => {
            if (err) {
                return cb(`error creating bucket ${bucketName}: ${err}\n`);
            }
            stderr.write(`done\n`);
            this.createdBucketsNb += 1;
            if (this.createdBucketsNb === this.nBuckets) {
                return cb();
            }
            this.createBuckets(cb);
        });
    }

    putObject(bucketName, key, data, callback) {
        const object = {
            Bucket: bucketName,
            Key: key,
            Body: data,
        };
        const storedKey = {
            Bucket: bucketName,
            Key: key,
        };
        this.s3.putObject(object, (err, value) => {
            if (!err) {
                return callback(null, value, storedKey);
            }
            stderr.write(`put ${key} in ${bucketName} NOK: `);
            stderr.write(`${err.code} ${err.message}\n`);
            callback(err, value);
        });
    }

    getObject(bucketName, key, callback) {
        const params = {
            Bucket: bucketName,
            Key: key,
        };
        this.s3.getObject(params, (err, data) => {
            if (!err) {
                return callback(null, data.Body);
            }
            stderr.write(`get ${key} in ${bucketName} NOK: `);
            stderr.write(`${err.code} ${err.message}\n`);
            callback(err);
        });
    }

    deleteObject(bucketName, key, callback) {
        const object = {
            Bucket: bucketName,
            Key: key,
        };
        this.s3.deleteObject(object, (err) => {
            if (!err) {
                return callback(null);
            }
            stderr.write(`delete ${key} in ${bucketName} NOK: `);
            stderr.write(`${err.code} ${err.message}\n`);
            callback(err);
        });
    }

    printStats(action) {
        const latMu = this.latSum / this.nSuccesses;
        const latSigma = Math.sqrt(this.latSumSq / this.nSuccesses
                                                            - latMu * latMu);
        stderr.write(`${action}  ${this.nSuccesses}  ${this.nFailures}     `);
        stderr.write(`${latMu.toFixed(3)}     ${latSigma.toFixed(5)}\n`);
    }

    resetStats() {
        this.count = 0;
        this.latSum = 0;
        this.latSumSq = 0;
        this.nBytes = 0;
        this.nSuccesses = 0;
        this.nFailures = 0;
        this.threads = 0;
    }

    updateStats(time) {
        const lat = time[0] * 1e3 + time[1] / 1e6;
        this.latSum += lat;
        this.latSumSq += lat * lat;
        this.nBytes += this.size;
        this.nSuccesses++;
    }

    isCorrectObject(key, src, data) {
        return (key === data.key) &&
               (Buffer.compare(new Buffer(data.data), src) === 0);
    }

    doSimul(cb) {
        stderr.write(`        #OK   #NOK  Average    Std. Dev.\n`);
        for (let i = 0; i < this.nThreads; i++) {
            if (this.actions[0]) {
                this.put(cb);
            } else if (this.actions[1]) {
                this.get(cb);
            } else if (this.actions[2]) {
                this.delete(cb);
            } else {
                return cb();
            }
        }
    }

    put(cb) {
        this.count++;
        if (this.count > this.nOps) {
            if (this.threads === 0) {
                this.printStats('   put');
                this.resetStats();
                if (this.actions[1]) {
                    this.get(cb);
                } else if (this.actions[2]) {
                    this.delete(cb);
                } else {
                    cb();
                    return;
                }
            }
            return;
        }
        if (this.threads > this.nThreads) {
            return;
        }
        this.threads++;
        const data = {
            key: `key${this.count}`,
            data: this.value,
        };
        const bucketName = this.bucketPrefix +
            Math.floor((Math.random() * this.nBuckets));
        const begin = process.hrtime();
        this.putObject(bucketName, data.key, JSON.stringify(data),
            function metrics(err, val, storedKey) {
                const end = process.hrtime(begin);
                this.threads--;
                if (!err) {
                    this.storedKeys.push(storedKey);
                    this.updateStats(end);
                } else {
                    this.nFailures++;
                    stderr.write(`put error: ${val}\n`);
                }
                this.put(cb);
            }.bind(this));
    }

    get(cb) {
        this.count++;
        if (this.count > this.storedKeys.length) {
            if (this.threads === 0) {
                this.printStats('   get');
                this.resetStats();
                if (this.actions[2]) {
                    this.delete(cb);
                } else {
                    cb();
                    return;
                }
            }
            return;
        }
        if (this.threads > this.nThreads) {
            return;
        }
        this.threads++;
        const storedKey = this.storedKeys[this.count - 1];
        const key = storedKey.Key;
        const bucketName = storedKey.Bucket;
        const begin = process.hrtime();
        this.getObject(bucketName, key, function metrics(err, data) {
            const end = process.hrtime(begin);
            this.threads--;
            if (!err &&
                this.isCorrectObject(key, this.value, JSON.parse(data))) {
                this.updateStats(end);
            } else {
                this.nFailures++;
                stderr.write(`get error: ${err}\n`);
            }
            this.get(cb);
        }.bind(this));
    }

    delete(cb) {
        if (this.storedKeys.length === 0) {
            if (this.threads === 0) {
                this.printStats('delete');
                this.resetStats();
                cb();
                return;
            }
            return;
        }
        if (this.threads > this.nThreads) {
            return;
        }
        this.threads++;
        const storedKey = this.storedKeys.shift();
        const key = storedKey.Key;
        const bucketName = storedKey.Bucket;
        const begin = process.hrtime();
        this.deleteObject(bucketName, key, function metrics(err) {
            const end = process.hrtime(begin);
            this.threads--;
            if (!err) {
                this.updateStats(end);
            } else {
                this.nFailures++;
                stderr.write(`delete error: ${err}\n`);
            }
            this.delete(cb);
        }.bind(this));
    }
}

describe('Measure serial PUT/GET/DELETE performance', function putPerf() {
    this.timeout(0);
    const blaster = new S3Blaster();
    before((done) => {
        blaster.createBuckets(done);
    });

    it('Only PUT', (done) => {
        blaster.setActions(true, false, false);
        blaster.doSimul(done);
    });

    it('Only GET', (done) => {
        blaster.setActions(false, true, false);
        blaster.doSimul(done);
    });

    it('Only DELETE', (done) => {
        blaster.setActions(false, false, true);
        blaster.doSimul(done);
    });

    it('PUT -> GET', (done) => {
        blaster.setActions(true, true, false);
        blaster.doSimul(done);
    });

    it('GET -> DELETE', (done) => {
        blaster.setActions(false, true, true);
        blaster.doSimul(done);
    });

    it('PUT -> DELETE', (done) => {
        blaster.setActions(true, false, true);
        blaster.doSimul(done);
    });

    it('PUT -> GET -> DELETE', (done) => {
        blaster.setActions(true, true, true);
        blaster.doSimul(done);
    });
});
