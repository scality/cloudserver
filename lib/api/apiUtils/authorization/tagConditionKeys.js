const assert = require('assert');
const async = require('async');
const { auth, s3middleware } = require('arsenal');
const metadata = require('../../../metadata/wrapper');
const { decodeVersionId } = require('../object/versioning');

const { parseTagXml } = s3middleware.tagging;

function makeTagQuery(tags) {
    return Object.entries(tags)
        .map(i => i.join('='))
        .join('&');
}

function updateRequestContexts(request, requestContexts, log, cb) {
    requestContexts.forEach(rc => {
        rc.setNeedTagEval(true);

        async.series([
            next => {
                if (request.headers['x-amz-tagging']) {
                    rc.setRequestObjTags(request.headers['x-amz-tagging']);
                    process.nextTick(() => next());
                } else if (request.post) {
                    parseTagXml(request.post, log, (err, tags) => {
                        if (err) {
                            log.trace('error parsing request tags');
                            return next(err);
                        }
                        rc.setRequestObjTags(makeTagQuery(tags));
                        return next();
                    });
                } else {
                    process.nextTick(() => next());
                }
            },
            next => {
                const objectKey = request.objectKey;
                const bucketName = request.bucketName;
                const decodedVidResult = decodeVersionId(request.query);
                if (decodedVidResult instanceof Error) {
                    log.trace('invalid versionId query', {
                        versionId: request.query.versionId,
                        error: decodedVidResult,
                    });
                    return process.nextTick(() => next(decodedVidResult));
                }
                const reqVersionId = decodedVidResult;
                return metadata.getObjectMD(bucketName, objectKey, { versionId: reqVersionId }, log,
                (err, objMD) => {
                    if (err) {
                        if (err.NoSuchKey) {
                            return next();
                        }
                        log.trace('error getting request object tags');
                        return next(err);
                    }
                    const existingTags = objMD.tags;
                    if (existingTags) {
                        rc.setExistingObjTag(makeTagQuery(existingTags));
                    }
                    return next();
                });
            },
        ], err => {
            if (err) {
                log.trace('error processing tag condition key evaluation');
                return cb(err);
            }
            return cb(null, requestContexts);
        });
    });
}

function tagConditionKeyAuth(authorizationResults, request, requestContexts, log, cb) {
    if (!authorizationResults) {
        return cb();
    }
    if (!authorizationResults.some(authRes => authRes.checkTagConditions)) {
        return cb();
    }

    return updateRequestContexts(request, requestContexts, log, (err, updatedContexts) => {
        if (err) {
            return cb(err);
        }
        if (assert.deepStrictEqual(requestContexts, updateRequestContexts)) {
            return cb();
        }
        return auth.server.doAuth(request, log,
            (err, userInfo, tagAuthResults) => cb(err, tagAuthResults), 's3', updatedContexts);
    });
}

module.exports = {
    tagConditionKeyAuth,
    updateRequestContexts,
    makeTagQuery,
};
