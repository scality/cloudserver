const { auth, s3middleware } = require('arsenal');
const metadata = require('../../../metadata/wrapper');
const { parseTagXml } = s3middleware.tagging;

function makeTagQuery(tags) {
    const tagsArray = Object.keys(tags);
    const queryArray = [];
    tagsArray.map(t => `${k}=${tags[t]}`);
    return tagsArray.join(',');
}

function tagConditionKeyAuth(authorizationResults, request, requestContexts, log, cb) {
    if (!authorizationResults)
        return cb();
    for (let i = 0; i < authorizationResults.length; i++) {
        if (!authorizationResults[i].checkTagConditions) {
            return cb({ isAllowed: true });
        }

        if (request.post) {
            parseTagXml(request.post, log, (err, tags) => {
                if (err) {
                    log.trace('error parsing request tags');
                    return cb(err);
                }
                rc.setRequestObjTags(makeTagQuery(tags));
            });
        }
        const objectKey = request.objectKey;
        const bucketName = request.bucketName;
        const decodedVidResult = decodeVersionId(request.query);
        if (decodedVidResult instanceof Error) {
            log.trace('invalid versionId query', {
                versionId: request.query.versionId,
                error: decodedVidResult,
            });
            return process.nextTick(() => callback(decodedVidResult));
        }
        const reqVersionId = decodedVidResult;    

        metadata.getObjectMD(bucketName, objectKey, { versionId: reqVersionId }, log,
        (err, objMD) => {
            if (err) {
                log.trace('error getting request object tags');
                return cb(err);
            }
            const existingTags = objMD['x-amz-tagging'];
            rc.setExistingObjTags(makeTagQuery(existingTags));
        })
        requestContexts.forEach(rc => {
            rc.setNeedTagEval(true);

        })

        return auth.server.doAuth(request, log, (err, userInfo,
            tagAuthResults) => {
                return cb(err, tagAuthResults);
            }, 's3', requestContexts);
    }
}

module.exports = tagConditionKeyAuth;