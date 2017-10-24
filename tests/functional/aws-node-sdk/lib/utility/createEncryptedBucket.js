const childProcess = require('child_process');
const Promise = require('bluebird');
const getConfig = require('../../test/support/config');

function safeJSONParse(s) {
    let res;
    try {
        res = JSON.parse(s);
    } catch (e) {
        return e;
    }
    return res;
}

function createEncryptedBucket(bucketParams, cb) {
    process.stdout.write('Creating encrypted bucket' +
    `${bucketParams.Bucket}`);
    const config = getConfig();
    const endpointWithoutHttp = config.endpoint.split('//')[1];
    const host = endpointWithoutHttp.split(':')[0];
    const port = endpointWithoutHttp.split(':')[1];
    let locationConstraint;
    if (bucketParams.CreateBucketConfiguration &&
        bucketParams.CreateBucketConfiguration.LocationConstraint) {
        locationConstraint = bucketParams.CreateBucketConfiguration
        .LocationConstraint;
    }

    const prog = `${__dirname}/../../../../../bin/create_encrypted_bucket.js`;
    let args = [
        prog,
        '-a', config.credentials.accessKeyId,
        '-k', config.credentials.secretAccessKey,
        '-b', bucketParams.Bucket,
        '-h', host,
        '-p', port,
        '-v',
    ];
    if (locationConstraint) {
        args = args.concat(['-l', locationConstraint]);
    }
    if (config.sslEnabled) {
        args = args.concat('-s');
    }
    const body = [];
    const child = childProcess.spawn(args[0], args)
    .on('exit', () => {
        const hasSucceed = body.join('').split('\n').find(item => {
            const json = safeJSONParse(item);
            const test = !(json instanceof Error) && json.name === 'S3' &&
                json.statusCode === 200;
            if (test) {
                return true;
            }
            return false;
        });
        if (!hasSucceed) {
            process.stderr.write(`${body.join('')}\n`);
            return cb(new Error('Cannot create encrypted bucket'));
        }
        return cb();
    })
    .on('error', cb);
    child.stdout.on('data', chunk => body.push(chunk.toString()));
}

const createEncryptedBucketPromise = Promise.promisify(createEncryptedBucket);

module.exports = {
    createEncryptedBucketPromise,
};
