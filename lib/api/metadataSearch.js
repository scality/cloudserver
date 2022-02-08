const { errors, versioning } = require('arsenal');
const constants = require('../../constants');
const services = require('../services');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { pushMetric } = require('../utapi/utilities');
const validateSearchParams = require('../api/apiUtils/bucket/validateSearch');
const parseWhere = require('../api/apiUtils/bucket/parseWhere');
const versionIdUtils = versioning.VersionID;
const monitoring = require('../utilities/monitoringHandler');
const { decryptToken }
    = require('../api/apiUtils/object/continueToken');
const { config } = require('../Config');
const { processVersions, processMasterVersions } = require('./bucketGet');


function handleResult(listParams, requestMaxKeys, encoding, authInfo,
                      bucketName, list, corsHeaders, log, callback) {
    // eslint-disable-next-line no-param-reassign
    listParams.maxKeys = requestMaxKeys;
    // eslint-disable-next-line no-param-reassign
    listParams.encoding = encoding;
    let res;
    if (listParams.listingType === 'DelimiterVersions') {
        res = processVersions(bucketName, listParams, list,
            config.versionIdEncodingType);
    } else {
        res = processMasterVersions(bucketName, listParams, list);
    }
    pushMetric('metadataSearch', log, { authInfo, bucket: bucketName });
    monitoring.promMetrics('GET', bucketName, '200', 'metadataSearch');
    return callback(null, res, corsHeaders);
}

/**
 * metadataSearch - Return list of objects in bucket that meet the search query, supports v1 & v2
 * @param  {AuthInfo} authInfo - Instance of AuthInfo class with
 *                               requester's info
 * @param  {object} request - http request object
 * @param  {function} log - Werelogs request logger
 * @param  {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
 */
function metadataSearch(authInfo, request, log, callback) {
    const params = request.query;
    const bucketName = request.bucketName;
    const v2 = params['list-type'];
    if (v2 !== undefined && Number.parseInt(v2, 10) !== 2) {
        return callback(errors.InvalidArgument.customizeDescription('Invalid ' +
            'List Type specified in Request'));
    }
    log.debug('processing request', { method: 'metadataSearch' });
    const encoding = params['encoding-type'];
    if (encoding !== undefined && encoding !== 'url') {
        monitoring.promMetrics(
            'GET', bucketName, 400, 'metadataSearch');
        return callback(errors.InvalidArgument.customizeDescription('Invalid ' +
            'Encoding Method specified in Request'));
    }
    const requestMaxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    if (Number.isNaN(requestMaxKeys) || requestMaxKeys < 0) {
        monitoring.promMetrics(
            'GET', bucketName, 400, 'metadataSearch');
        return callback(errors.InvalidArgument);
    }
    // AWS only returns 1000 keys even if max keys are greater.
    // Max keys stated in response xml can be greater than actual
    // keys returned.
    const actualMaxKeys = Math.min(constants.listingHardLimit, requestMaxKeys);

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'metadataSearch',
        request,
    };
    const listParams = {
        listingType: 'DelimiterMaster',
        maxKeys: actualMaxKeys,
        delimiter: params.delimiter,
        prefix: params.prefix,
    };
    try {
        const validatedAst = validateSearchParams(params.search).ast;
        listParams.mongifiedSearch = parseWhere(validatedAst);
    } catch (err) {
        log.debug(err.message, {
            stack: err.stack,
        });
        monitoring.promMetrics(
            'GET', bucketName, 400, 'metadataSearch');
        return callback(errors.InvalidArgument
            .customizeDescription('Invalid sql where clause ' +
                'sent as search query'));
    }
    if (v2) {
        listParams.v2 = true;
        listParams.startAfter = params['start-after'];
        listParams.continuationToken =
            decryptToken(params['continuation-token']);
        listParams.fetchOwner = params['fetch-owner'] === 'true';
    } else {
        listParams.marker = params.marker;
    }

    metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', { error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'metadataSearch');
            return callback(err, null, corsHeaders);
        }
        if (params.versions !== undefined) {
            listParams.listingType = 'DelimiterVersions';
            delete listParams.marker;
            listParams.keyMarker = params['key-marker'];
            listParams.versionIdMarker = params['version-id-marker'] ?
                versionIdUtils.decode(params['version-id-marker']) : undefined;
        }
        if (!requestMaxKeys) {
            const emptyList = {
                CommonPrefixes: [],
                Contents: [],
                Versions: [],
                IsTruncated: false,
            };
            return handleResult(listParams, requestMaxKeys, encoding, authInfo,
                bucketName, emptyList, corsHeaders, log, callback);
        }
        return services.getObjectListing(bucketName, listParams, log,
            (err, list) => {
                if (err) {
                    log.debug('error processing request', { error: err });
                    monitoring.promMetrics(
                        'GET', bucketName, err.code, 'metadataSearch');
                    return callback(err, null, corsHeaders);
                }
                return handleResult(listParams, requestMaxKeys, encoding, authInfo,
                    bucketName, list, corsHeaders, log, callback);
            });
    });
    return undefined;
}

module.exports = metadataSearch;
