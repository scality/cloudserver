const { isServiceAccount, getServiceAccountProperties } = require('../authorization/permissionChecks');
const { constants, models } = require('arsenal');

const { replicationBackends } = constants;
const { ReplicationConfiguration } = models;

/**
 * Apply the default replication endpoint as a fallback storageClass
 * for rules that don't specify one. Returns a new rules array; rules
 * without a resolvable storageClass are dropped so they never reach
 * the backend resolver.
 */
function _withDefaultStorageClass(rules, s3config) {
    const { replicationEndpoints = [] } = s3config;
    const fallback = replicationEndpoints.find(e => e.default)?.site ?? replicationEndpoints[0]?.site;
    return rules
        .map(rule => {
            if (rule.storageClass) {
                return rule;
            }
            if (!fallback) {
                return null;
            }
            return { ...rule, storageClass: fallback };
        })
        .filter(Boolean);
}

/**
 * Check whether the authenticated user is allowed to trigger replication.
 * Internal service accounts (e.g. Lifecycle) are not allowed unless their
 * account properties explicitly permit it (e.g. MD ingestion).
 * @param {AuthInfo} [authInfo] - authentication info of the request issuer
 * @return {boolean} true if the user can trigger replication
 */
function _canUserReplicate(authInfo) {
    if (!authInfo) {
        return true;
    }
    const canonicalId = authInfo.getCanonicalID();
    if (!isServiceAccount(canonicalId)) {
        return true;
    }
    const props = getServiceAccountProperties(canonicalId);
    return !!props?.canReplicate;
}

/**
 * Get the object replicationInfo to replicate data and metadata, or only
 * metadata if the operation only changes metadata or the object is 0 bytes.
 *
 * The rule-matching / dedup / per-backend stamping logic lives in
 * arsenal's `ReplicationConfiguration.resolveBackends`. This function
 * is the cloudserver-specific shell: it enforces the service-account
 * gate, supplies a default storageClass from `replicationEndpoints`,
 * decides the `content` array based on the operation kind, and
 * stitches the result into a `replicationInfo` envelope.
 *
 * @param {object} params - Named parameters
 * @param {object} params.s3config - Cloudserver configuration object
 * @param {object} params.s3config.locationConstraints - Configured map of location constraints
 * @param {object[]} params.s3config.replicationEndpoints - Configured replication endpoints
 * @param {string} params.objKey - The key of the object
 * @param {object} params.bucketMD - The bucket metadata
 * @param {boolean} params.isMD - Whether the operation is only updating metadata
 * @param {number} params.objSize - The size, in bytes, of the object being PUT
 * @param {string} [params.operationType] - The type of operation to replicate
 * @param {object} [params.objectMD] - The object metadata
 * @param {AuthInfo} [params.authInfo] - authentication info of object owner
 * @param {string[]} [params.blockedSiteTypes=[]] - location types to exclude from the returned backends
 * @return {object|undefined}
 */
function getReplicationInfo(params) {
    const {
        s3config,
        objKey,
        bucketMD,
        isMD,
        objSize,
        operationType,
        objectMD,
        authInfo,
        blockedSiteTypes = [],
    } = params;

    const config = bucketMD.getReplicationConfiguration();
    if (!config || !_canUserReplicate(authInfo)) {
        return undefined;
    }

    const isCloud = site => !!replicationBackends[s3config.locationConstraints[site]?.type];
    const rules = _withDefaultStorageClass(config.rules || [], s3config);
    const allBackends = ReplicationConfiguration.resolveBackends(
        { ...config, rules },
        objKey,
        isCloud,
        objectMD?.replicationInfo?.backends,
    );

    const backends =
        blockedSiteTypes.length > 0
            ? allBackends.filter(b => {
                  const loc = s3config.locationConstraints[b.site];
                  return !loc || !blockedSiteTypes.includes(loc.type);
              })
            : allBackends;

    if (backends.length === 0) {
        return undefined;
    }

    const hasCloudBackend = backends.some(b => isCloud(b.site));

    const content = isMD || objSize === 0 ? ['METADATA'] : ['DATA', 'METADATA'];
    if (hasCloudBackend && operationType) {
        content.push(operationType);
    }

    return {
        status: 'PENDING',
        backends,
        content,
        role: ReplicationConfiguration.resolveSourceRole(config.role),
        isNFS: bucketMD.isNFS(),
        // `undefined` so JSON/BSON drop the field on persist (saves bytes).
        isReplica: undefined,
    };
}

module.exports = getReplicationInfo;
