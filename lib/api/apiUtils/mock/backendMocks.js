// Environment variable configuration
const ENABLE_DOAUTH_MOCK = process.env.MOCK_DOAUTH === 'true';
const ENABLE_METADATA_MOCK = process.env.MOCK_METADATA === 'true';
const ENABLE_STORE_OBJECT_MOCK = process.env.MOCK_STORE_OBJECT === 'true';

const DOAUTH_DELAY_MS = parseInt(process.env.MOCK_DOAUTH_DELAY_MS, 10) || 10;
const METADATA_DELAY_MS = parseInt(process.env.MOCK_METADATA_DELAY_MS, 10) || 8;
const STORE_OBJECT_DELAY_MS = parseInt(process.env.MOCK_STORE_OBJECT_DELAY_MS, 10) || 6;

// Cache for storing results from real function calls
const mockCache = {
    doAuth: null,
    metadata: null,
    storeObject: null,
};

// Track if we've made the initial real call
const hasCalledReal = {
    doAuth: false,
    metadata: false,
    storeObject: false,
};

/**
 * Mock for doAuth function
 * @param {Function} realDoAuthFn - The real doAuth function to call once
 * @param {Object} request - Request object
 * @param {Object} log - Logger object
 * @param {Function} callback - Callback function
 * @param {string} source - Source parameter
 * @param {Object} requestContexts - Request contexts
 */
function mockDoAuth(realDoAuthFn, request, log, callback, source, requestContexts) {
    if (!ENABLE_DOAUTH_MOCK) {
        return realDoAuthFn(request, log, callback, source, requestContexts);
    }

    if (!hasCalledReal.doAuth) {
        return realDoAuthFn(request, log, (err, userInfo, authorizationResults, streamingV4Params, infos) => {
            if (!err) {
                mockCache.doAuth = { userInfo, authorizationResults, streamingV4Params, infos };
                log.debug('doAuth mock: cached real result', { 
                    hasUserInfo: !!userInfo,
                    hasAuthResults: !!authorizationResults 
                });
            } else {
                log.debug('doAuth mock: caching error result', { error: err.message });
                mockCache.doAuth = { error: err };
            }
            // First call - call the real function and cache the result
            hasCalledReal.doAuth = true;
            callback(err, userInfo, authorizationResults, streamingV4Params, infos);
        }, source, requestContexts);
    }

    // Subsequent calls - return cached result after delay
    log.debug('doAuth mock: returning cached result after delay', { delayMs: DOAUTH_DELAY_MS });
    
    setTimeout(() => {
        const cached = mockCache.doAuth;
        if (!cached) {
            return callback(new Error('No cached result found'));
        }
        if (cached.error) {
            callback(cached.error);
        } else {
            callback(null, cached.userInfo, cached.authorizationResults, cached.streamingV4Params, cached.infos);
        }
    }, DOAUTH_DELAY_MS);
}

/**
 * Mock for standardMetadataValidateBucketAndObj function
 * @param {Function} realMetadataFn - The real metadata function to call once
 * @param {Object} params - Parameters object
 * @param {Object} actionImplicitDenies - Action implicit denies
 * @param {Object} log - Logger object
 * @param {Function} callback - Callback function
 */
function mockStandardMetadataValidateBucketAndObj(realMetadataFn, params, actionImplicitDenies, log, callback) {
    if (!ENABLE_METADATA_MOCK) {
        return realMetadataFn(params, actionImplicitDenies, log, callback);
    }

    const cacheKey = `${params.bucketName}-${params.objectKey}-${params.versionId || 'null'}-${params.requestType}`;
    
    if (!hasCalledReal.metadata) {
        return realMetadataFn(params, actionImplicitDenies, log, (err, bucket, objMD) => {
            if (!err) {
                mockCache.metadata = { bucket, objMD };
                log.debug('metadata mock: cached real result', { 
                    bucketName: params.bucketName,
                    hasObjMD: !!objMD 
                });
            } else {
                log.debug('metadata mock: caching error result', { error: err.message });
                mockCache.metadata = { error: err, bucket };
            }
            // First call - call the real function and cache the result
            hasCalledReal.metadata = true;
            callback(err, bucket, objMD);
        });
    }

    // Subsequent calls - return cached result after delay
    log.debug('metadata mock: returning cached result after delay', { 
        delayMs: METADATA_DELAY_MS,
        cacheKey 
    });
    
    setTimeout(() => {
        const cached = mockCache.metadata;
        if (!cached) {
            return callback(new Error('No cached result found'));
        }
        if (cached.error) {
            callback(cached.error, cached.bucket);
        } else {
            callback(null, cached.bucket, cached.objMD);
        }
    }, METADATA_DELAY_MS);
}

/**
 * Mock for createAndStoreObject function
 * @param {Function} realStoreObjectFn - The real store object function to call once
 * @param {string} bucketName - Bucket name
 * @param {Object} bucketMD - Bucket metadata
 * @param {string} objectKey - Object key
 * @param {Object} objMD - Object metadata
 * @param {Object} authInfo - Auth info
 * @param {string} canonicalID - Canonical ID
 * @param {Object} cipherBundle - Cipher bundle
 * @param {Object} request - Request object
 * @param {boolean} isDeleteMarker - Is delete marker
 * @param {Object} streamingV4Params - Streaming V4 params
 * @param {Object} overheadField - Overhead field
 * @param {Object} log - Logger object
 * @param {string} originOp - Origin operation
 * @param {Function} callback - Callback function
 */
function mockCreateAndStoreObject(realStoreObjectFn, bucketName, bucketMD, objectKey, objMD, authInfo,
    canonicalID, cipherBundle, request, isDeleteMarker, streamingV4Params,
    overheadField, log, originOp, callback) {
    
    if (!ENABLE_STORE_OBJECT_MOCK) {
        return realStoreObjectFn(bucketName, bucketMD, objectKey, objMD, authInfo,
            canonicalID, cipherBundle, request, isDeleteMarker, streamingV4Params,
            overheadField, log, originOp, callback);
    }

    if (!hasCalledReal.storeObject) {
        return realStoreObjectFn(bucketName, bucketMD, objectKey, objMD, authInfo,
            canonicalID, cipherBundle, request, isDeleteMarker, streamingV4Params,
            overheadField, log, originOp, (err, result) => {
                if (!err) {
                    mockCache.storeObject = { result };
                    log.debug('storeObject mock: cached real result', { 
                        bucketName,
                        objectKey,
                        hasResult: !!result 
                    });
                } else {
                    log.debug('storeObject mock: caching error result', { error: err.message });
                    mockCache.storeObject = { error: err };
                }
                // First call - call the real function and cache the result
                hasCalledReal.storeObject = true;
                callback(err, result);
            });
    }

    // Subsequent calls - return cached result after delay
    log.debug('storeObject mock: returning cached result after delay', { 
        delayMs: STORE_OBJECT_DELAY_MS,
        bucketName,
        objectKey 
    });
    
    setTimeout(() => {
        const cached = mockCache.storeObject;
        if (!cached) {
            return callback(new Error('No cached result found'));
        }
        if (cached.error) {
            callback(cached.error);
        } else {
            callback(null, cached.result);
        }
    }, STORE_OBJECT_DELAY_MS);
}

/**
 * Reset all mock caches (useful for testing)
 */
function resetMockCaches() {
    mockCache.doAuth = null;
    mockCache.metadata = null;
    mockCache.storeObject = null;
    hasCalledReal.doAuth = false;
    hasCalledReal.metadata = false;
    hasCalledReal.storeObject = false;
}

/**
 * Get current mock configuration
 */
function getMockConfig() {
    return {
        doAuth: {
            enabled: ENABLE_DOAUTH_MOCK,
            delayMs: DOAUTH_DELAY_MS,
            hasCached: !!mockCache.doAuth,
        },
        metadata: {
            enabled: ENABLE_METADATA_MOCK,
            delayMs: METADATA_DELAY_MS,
            hasCached: !!mockCache.metadata,
        },
        storeObject: {
            enabled: ENABLE_STORE_OBJECT_MOCK,
            delayMs: STORE_OBJECT_DELAY_MS,
            hasCached: !!mockCache.storeObject,
        },
    };
}

module.exports = {
    mockDoAuth,
    mockStandardMetadataValidateBucketAndObj,
    mockCreateAndStoreObject,
    resetMockCaches,
    getMockConfig,
    // Export the configuration flags for other modules to check
    ENABLE_DOAUTH_MOCK,
    ENABLE_METADATA_MOCK,
    ENABLE_STORE_OBJECT_MOCK,
}; 
