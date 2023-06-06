const { supportedLifecycleRules } = require('arsenal').constants;
const { LifecycleConfiguration } = require('arsenal').models;
const {
    LifecycleDateTime,
    LifecycleUtils,
} = require('arsenal').s3middleware.lifecycleHelpers;
const { config } = require('../../../Config');

const {
    expireOneDayEarlier,
    transitionOneDayEarlier,
    timeProgressionFactor,
    scaledMsPerDay,
} = config.getTimeOptions();

const lifecycleDateTime = new LifecycleDateTime({
    transitionOneDayEarlier,
    expireOneDayEarlier,
    timeProgressionFactor,
});

const lifecycleUtils = new LifecycleUtils(supportedLifecycleRules, lifecycleDateTime, timeProgressionFactor);

function calculateDate(objDate, expDays, datetime) {
    return new Date(datetime.getTimestamp(objDate) + (expDays * scaledMsPerDay));
}

function formatExpirationHeader(date, id) {
    return `expiry-date="${date}", rule-id="${encodeURIComponent(id)}"`;
}

// format: x-amz-expiration: expiry-date="Fri, 21 Dec 2012 00:00:00 GMT", rule-id="id"
const AMZ_EXP_HEADER = 'x-amz-expiration';
// format: x-amz-abort-date: "Fri, 21 Dec 2012 00:00:00 GMT"
const AMZ_ABORT_DATE_HEADER = 'x-amz-abort-date';
// format: x-amz-abort-rule-id: "rule id"
const AMZ_ABORT_ID_HEADER = 'x-amz-abort-rule-id';


function _generateExpHeadersObjects(rules, params, datetime) {
    const tags = {
        TagSet: Object.keys(params.tags)
            .map(key => ({ Key: key, Value: params.tags[key] })),
    };

    const objectInfo = { Key: params.key };
    const filteredRules = lifecycleUtils.filterRules(rules, objectInfo, tags);
    const applicable = lifecycleUtils.getApplicableRules(filteredRules, objectInfo, datetime);

    if (applicable.Expiration) {
        const rule = applicable.Expiration;

        if (rule.Days === undefined && rule.Date === undefined) {
            return {};
        }

        if (rule.Date) {
            return {
                [AMZ_EXP_HEADER]: formatExpirationHeader(rule.Date, rule.ID),
            };
        }

        const date = calculateDate(params.date, rule.Days, datetime);
        return {
            [AMZ_EXP_HEADER]: formatExpirationHeader(date.toUTCString(), rule.ID),
        };
    }

    return {};
}

function _generateExpHeadresMPU(rules, params, datetime) {
    const noTags = { TagSet: [] };

    const objectInfo = { Key: params.key };

    const filteredRules = lifecycleUtils.filterRules(rules, objectInfo, noTags);
    const applicable = lifecycleUtils.getApplicableRules(filteredRules, {}, datetime);

    if (applicable.AbortIncompleteMultipartUpload) {
        const rule = applicable.AbortIncompleteMultipartUpload;
        const date = calculateDate(
            params.date,
            rule.DaysAfterInitiation,
            datetime
        );

        return {
            [AMZ_ABORT_ID_HEADER]: encodeURIComponent(rule.ID),
            [AMZ_ABORT_DATE_HEADER]: date.toUTCString(),
        };
    }

    return {};
}

/**
 * generate response expiration headers
 * @param {object} params - params
 * @param {LifecycleDateTime} datetime - lifecycle datetime object
 * @returns {object} - expiration response headers
 */
function generateExpirationHeaders(params, datetime) {
    const { lifecycleConfig, objectParams, mpuParams, isVersionedReq } = params;

    if (!lifecycleConfig || isVersionedReq) {
        return {};
    }

    const lcfg = LifecycleConfiguration.getConfigJson(lifecycleConfig);

    if (objectParams) {
        return _generateExpHeadersObjects(lcfg.Rules, objectParams, datetime);
    }

    if (mpuParams) {
        return _generateExpHeadresMPU(lcfg.Rules, mpuParams, datetime);
    }

    return {};
}

/**
 * set response expiration headers to target header object
 * @param {object} headers - target header object
 * @param {object} params - params
 * @returns {undefined}
 */
function setExpirationHeaders(headers, params) {
    const expHeaders = generateExpirationHeaders(params, lifecycleDateTime);
    Object.assign(headers, expHeaders);
}

module.exports = {
    lifecycleDateTime,
    generateExpirationHeaders,
    setExpirationHeaders,
};
