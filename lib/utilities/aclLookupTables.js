/**
 * Pre-compiled ACL lookup tables for fast permission checking
 * These eliminate string comparisons and array searches during request processing
 */

const constants = require('../../constants');

// Pre-compiled lookup tables
const VALID_CANNED_ACLS = {
    bucket: new Set(['private', 'public-read', 'public-read-write', 'authenticated-read', 'log-delivery-write']),
    object: new Set(['private', 'public-read', 'public-read-write', 'authenticated-read',
        'bucket-owner-read', 'bucket-owner-full-control'])
};

const VALID_GROUPS = new Set([
    constants.allAuthedUsersId,
    constants.publicId,
    constants.logId,
]);

const GRANT_HEADER_MAP = {
    'x-amz-grant-read': 'READ',
    'x-amz-grant-write': 'WRITE', 
    'x-amz-grant-read-acp': 'READ_ACP',
    'x-amz-grant-write-acp': 'WRITE_ACP',
    'x-amz-grant-full-control': 'FULL_CONTROL'
};

const GRANT_HEADERS = Object.keys(GRANT_HEADER_MAP);

// Pre-compiled canned ACL templates (avoid object creation)
const CANNED_ACL_TEMPLATES = {
    'private': {
        Canned: 'private',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    'public-read': {
        Canned: 'public-read', 
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    'public-read-write': {
        Canned: 'public-read-write',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    'authenticated-read': {
        Canned: 'authenticated-read',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    'log-delivery-write': {
        Canned: 'log-delivery-write',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    'bucket-owner-read': {
        Canned: 'bucket-owner-read',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    'bucket-owner-full-control': {
        Canned: 'bucket-owner-full-control',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    }
};

/**
 * Fast canned ACL validation using pre-compiled Set lookup
 * @param {string} cannedAcl - The canned ACL to validate
 * @param {string} resourceType - 'bucket' or 'object'
 * @returns {boolean} - true if valid
 */
function isValidCannedAcl(cannedAcl, resourceType) {
    return VALID_CANNED_ACLS[resourceType]?.has(cannedAcl) || false;
}

/**
 * Get pre-compiled canned ACL template (fast object reuse)
 * @param {string} cannedAcl - The canned ACL name
 * @returns {object} - ACL template object
 */
function getCannedAclTemplate(cannedAcl) {
    return CANNED_ACL_TEMPLATES[cannedAcl] || null;
}

/**
 * Fast group validation using pre-compiled Set
 * @param {string} groupId - Group identifier
 * @returns {boolean} - true if valid group
 */
function isValidGroup(groupId) {
    return VALID_GROUPS.has(groupId);
}

/**
 * Check if headers contain any grant headers (fast pre-check)
 * @param {object} headers - Request headers
 * @returns {boolean} - true if any grant headers present
 */
function hasAnyGrantHeaders(headers) {
    return GRANT_HEADERS.some(header => headers[header]);
}

/**
 * Extract all grant headers efficiently 
 * @param {object} headers - Request headers
 * @returns {Array} - Array of {header, type, value} objects
 */
function extractGrantHeaders(headers) {
    const grants = [];
    for (const header of GRANT_HEADERS) {
        if (headers[header]) {
            grants.push({
                header,
                type: GRANT_HEADER_MAP[header],
                value: headers[header]
            });
        }
    }
    return grants;
}

module.exports = {
    VALID_CANNED_ACLS,
    VALID_GROUPS,
    GRANT_HEADER_MAP,
    GRANT_HEADERS,
    CANNED_ACL_TEMPLATES,
    isValidCannedAcl,
    getCannedAclTemplate,
    isValidGroup,
    hasAnyGrantHeaders,
    extractGrantHeaders,
}; 
