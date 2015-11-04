'use strict'; // eslint-disable-line strict

const proc = require('child_process');
const process = require('process');
const assert = require('assert');

const program = 's3cmd';
const upload = 'package.json';
const download = 'tmpfile';
const bucket = 'universe';
const nonexist = 'VOID';

const isIronman = process.env.IP ? ['-c', `${__dirname}/s3cfg`, ] : null;

function diff(done) {
    process.stdout.write(`diff ${upload} ${download}\n`);
    proc.spawn('diff', [ upload, download, ]).on('exit', code => {
        proc.spawn('rm', [ download, ]);
        assert.deepEqual(code, 0);
        done();
    });
}

function exec(args, done, exitCode) {
    let exit = exitCode;
    if (exit === undefined) {
        exit = 0;
    }
    let av = args;
    if (isIronman) {
        av = args.concat(isIronman);
    }
    process.stdout.write(`${program} ${av}\n`);
    proc.spawn(program, av, { stdio: 'inherit' })
        .on('exit', code => {
            assert.deepEqual(code, exit);
            done();
        });
}

describe('s3cmd putBucket', () => {
    it('should put a valid bucket', (done) => {
        exec(['mb', `s3://${bucket}`, ], done);
    });

    it('put the same bucket, should fail', (done) => {
        exec(['mb', `s3://${bucket}`, ], done, 13);
    });

    it('put an invalid bucket, should fail', (done) => {
        exec(['mb', `s3://${nonexist}`, ], done, 11);
    });
});

describe('s3cmd getBucket', () => {
    it('should list existing bucket', (done) => {
        exec(['ls', `s3://${bucket}`, ], done);
    });

    it('list non existing bucket, should fail', (done) => {
        exec(['ls', `s3://${nonexist}`, ], done, 12);
    });
});

describe('s3cmd putObject', () => {
    it('put file in existing bucket', (done) => {
        exec(['put', upload, `s3://${bucket}`, ], done);
    });

    it('should put file with the same name in existing bucket', (done) => {
        exec(['put', upload, `s3://${bucket}`, ], done);
    });

    it('put file in non existing bucket, should fail', (done) => {
        exec(['put', upload, `s3://${nonexist}`, ], done, 12);
    });
});

describe('s3cmd getObject', () => {
    it('should get existing file in existing bucket', (done) => {
        exec(['get', `s3://${bucket}/${upload}`, download ], done);
    });

    it('downloaded file should equal uploaded file', (done) => {
        diff(done);
    });

    it('get non existing file in existing bucket, should fail', (done) => {
        exec(['get', `s3://${bucket}/${nonexist}`, download, ], done, 12);
    });

    it('get file in non existing bucket, should fail', (done) => {
        exec(['get', `s3://${nonexist}/${nonexist}`, download, ], done, 12);
    });
});

describe('s3cmd delObject', () => {
    it('should delete existing object', (done) => {
        exec(['rm', `s3://${bucket}/${upload}`, ], done);
    });

    it('delete non existing object, should fail', (done) => {
        exec(['rm', `s3://${bucket}/${nonexist}`, ], done, 12);
    });

    it('try to get the deleted object, should fail', (done) => {
        exec(['get', `s3://${bucket}/${upload}`, download, ], done, 12);
    });
});

describe('connector edge cases', () => {
    it('should put previous file in existing bucket', (done) => {
        exec(['put', upload, `s3://${bucket}`, ], done);
    });

    it('should get existing file in existing bucket', (done) => {
        exec(['get', `s3://${bucket}/${upload}`, download ], done);
    });
});

describe('s3cmd delBucket', () => {
    it('delete non-empty bucket, should fail', (done) => {
        exec(['rb', `s3://${bucket}`, ], done, 13);
    });

    it('should delete remaining object', (done) => {
        exec(['rm', `s3://${bucket}/${upload}`, ], done);
    });

    it('should delete empty bucket', (done) => {
        exec(['rb', `s3://${bucket}`, ], done);
    });

    it('try to get the deleted bucket, should fail', (done) => {
        exec(['ls', `s3://${bucket}`, ], done, 12);
    });
});
