const assert = require('assert');
const async = require('async');
const { versioning } = require('arsenal');

const { metadataGetObject } = require('../../../../../lib/metadata/metadataUtils');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const versionIdUtils = versioning.VersionID;

const log = new DummyRequestLogger();

function removeObjectLock(objects, cb) {
    async.each(objects, (object, next) => {
        const { bucket, key, versionId } = object;
        metadataGetObject(bucket, key, versionIdUtils.decode(versionId), log, (err, objMD) => {
            assert.ifError(err);
            /* eslint-disable no-param-reassign */
            objMD.retentionMode = '';
            objMD.retentionDate = '';
            objMD.legalHold = false;
            metadata.putObjectMD(bucket, key, objMD, { versionId: objMD.versionId }, log, err => {
                assert.ifError(err);
                next();
            });
        });
    }, cb);
}

module.exports = removeObjectLock;
