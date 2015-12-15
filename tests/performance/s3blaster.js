import crypto from 'crypto';
import { config, S3 } from 'aws-sdk';
import commander from 'commander';

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
        config.endpoint = this.host + ':' + this.port;
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
    }

    createBucket(bucketName, callback) {
        this.s3.createBucket({ Bucket: bucketName }, (err) => {
            if (!err) {
                return callback(true);
            }
            const code = err.toString().split(':')[0];
            stderr.write(`createBucket: ${code}\n`);
            callback(code === 'BucketAlreadyExists' ? true : false);
        });
    }

    putObject(bucketName, key, data, callback) {
        const object = {
            Bucket: bucketName,
            Key: key,
            Body: data,
        };
        this.s3.putObject(object, (err, value) => {
            if (!err) {
                return callback(true, value);
            }
            stderr.write(`putObj NOK: ${err.code} ${err.message} ${value}\n`);
            callback(false, value);
        });
    }

    printStats() {
        const latMu = this.latSum / this.nSuccesses;
        const latSigma = Math.sqrt(this.latSumSq / this.nSuccesses
                                   - latMu * latMu);
        stderr.write(`nSuccesses: ${this.nSuccesses}\n`);
        stderr.write(`nFailures: ${this.nFailures}\n`);
        stderr.write(`total nbytes: ${this.nBytes}B\n`);
        stderr.write(`avg: ${latMu}ms\n`);
        stderr.write(`std dev: ${latSigma}ms\n`);
    }

    put(i) {
        if (i > this.nOps) {
            if (this.threads === 0) {
                this.printStats();
            }
            return;
        }
        if (this.threads > this.nThreads) {
            return;
        }
        this.threads++;
        const data = {
            key: `key${i}`,
            data: this.value,
        };
        const bucketName = this.bucketPrefix +
            Math.floor((Math.random() * this.nBuckets));
        const begin = process.hrtime();
        this.putObject(bucketName, data.key, JSON.stringify(data),
            function metrics(ok, val) {
                const end = process.hrtime(begin);
                this.threads--;
                if (i % 1000 === 0) {
                    stderr.write(`${i}\n`);
                }
                if (ok) {
                    const lat = end[0] * 1e3 + end[1] / 1e6;
                    this.latSum += lat;
                    this.latSumSq += lat * lat;
                    this.nBytes += this.size;
                    this.nSuccesses++;
                } else {
                    this.nFailures++;
                    stderr.write(`error: ${val}\n`);
                }
                setTimeout(this.put.bind(this), 0, ++this.count);
            }.bind(this));
    }

    bucketCallback(ok) {
        if (ok) {
            this.okBucket++;
            if (this.okBucket === this.nBuckets) {
                for (let i = 0; i < this.nThreads; i++) {
                    stderr.write(`started thread ${i}\n`);
                    this.put(++this.count);
                }
            }
        } else {
            stderr.write('error creating bucket\n');
            process.exit(1);
        }
    }
}

function main() {
    const blaster = new S3Blaster();
    for (let i = 0; i < blaster.nBuckets; i++) {
        const bucketName = `${blaster.bucketPrefix}${i}`;
        stderr.write(`creating bucket ${bucketName}\n`);
        blaster.createBucket(bucketName, blaster.bucketCallback.bind(blaster));
    }
}

main();
