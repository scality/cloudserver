const { errorInstances } = require('arsenal');
const { getPartCountFromMd5 } = require('./partInfo');
const { algorithms } = require('../integrity/validateChecksums');

/**
 * Parse and validate attribute headers from a request.
 * @param {object} headers - Request headers object
 * @param {string} headerName - Name of the header to parse (e.g., 'x-amz-object-attributes')
 * @param {Set<string>} supportedAttributes - Set of valid attribute names
 * @returns {Set<string>} - set of requested attribute names
 * @throws {arsenal.errors.InvalidRequest} When header is required but missing/empty
 * @throws {arsenal.errors.InvalidArgument} When an invalid attribute name is specified
 * @example
 * // Input headers:
 * { 'headerName': 'ETag, ObjectSize, x-amz-meta-custom' }
 *
 * // Parsed result:
 * ['ETag', 'ObjectSize', 'x-amz-meta-custom']
 */
function parseAttributesHeaders(headers, headerName, supportedAttributes) {
    const result = new Set();

    const rawValue = headers[headerName];
    if (rawValue === null || rawValue === undefined) {
        return result;
    }

    for (const rawAttr of rawValue.split(',')) {
        let attr = rawAttr.trim();

        if (!supportedAttributes.has(attr)) {
            attr = attr.toLowerCase();
        }

        if (!attr.startsWith('x-amz-meta-') && !supportedAttributes.has(attr)) {
            throw errorInstances.InvalidArgument.customizeDescription('Invalid attribute name specified.');
        }

        result.add(attr);
    }

    return result;
}

/**
 * buildAttributesXml - Builds XML reponse for requested object attributes
 * @param {Object} objectMD - The internal metadata object for the file/object.
 * @param {string} [objectMD.content-md5] - The MD5 hash used for the ETag.
 * @param {string} [objectMD.x-amz-storage-class] - The storage tier of the object.
 * @param {number} [objectMD.content-length] - The size of the object in bytes.
 * @param {Object} [objectMD.restoreStatus] - Information regarding the restoration of archived objects.
 * @param {boolean} [objectMD.restoreStatus.inProgress] - Whether a restore is currently active.
 * @param {string} [objectMD.restoreStatus.expiryDate] - The date after which the restored copy expires.
 * @param {Object.<string, any>} userMetadata - Key-value pairs of user-defined metadata.
 * @param {string[]} requestedAttrs - A list of specific attributes to include in the output.
 * Supports 'ETag', 'ObjectParts', 'StorageClass', 'ObjectSize',
 * 'Checksum', 'RestoreStatus', and 'x-amz-meta-*' for all user metadata.
 * @param {string[]} xml - The string array acting as the output buffer/collector.
 * @param {object} log - Werelogs logger.
 * @returns {void} - this function does not return a value, it mutates the `xml` param.
 */
function buildAttributesXml(objectMD, userMetadata, requestedAttrs, xml, log) {
    const customAttributes = new Set();
    for (const attribute of requestedAttrs) {
        switch (attribute) {
            case 'ETag':
                xml.push(`<ETag>${objectMD['content-md5']}</ETag>`);
                break;
            case 'ObjectParts': {
                const partCount = getPartCountFromMd5(objectMD);
                if (partCount) {
                    xml.push('<ObjectParts>', `<PartsCount>${partCount}</PartsCount>`, '</ObjectParts>');
                }
                break;
            }
            case 'StorageClass':
                xml.push(`<StorageClass>${objectMD['x-amz-storage-class']}</StorageClass>`);
                break;
            case 'ObjectSize':
                xml.push(`<ObjectSize>${objectMD['content-length']}</ObjectSize>`);
                break;
            case 'Checksum': {
                const { checksum } = objectMD;
                if (checksum) {
                    const algo = algorithms[checksum.checksumAlgorithm];
                    if (!algo) {
                        log.error('unknown checksum algorithm in object metadata', {
                            checksumAlgorithm: checksum.checksumAlgorithm,
                        });
                        break;
                    }
                    const tag = algo.getObjectAttributesXMLTag;
                    xml.push(
                        '<Checksum>',
                        `<${tag}>${checksum.checksumValue}</${tag}>`,
                        `<ChecksumType>${checksum.checksumType}</ChecksumType>`,
                        '</Checksum>',
                    );
                }
                break;
            }
            case 'RestoreStatus':
                xml.push('<RestoreStatus>');
                xml.push(`<IsRestoreInProgress>${!!objectMD.restoreStatus?.inProgress}</IsRestoreInProgress>`);

                if (objectMD.restoreStatus?.expiryDate) {
                    xml.push(`<RestoreExpiryDate>${objectMD.restoreStatus?.expiryDate}</RestoreExpiryDate>`);
                }

                xml.push('</RestoreStatus>');
                break;
            case 'x-amz-meta-*':
                for (const key of Object.keys(userMetadata)) {
                    customAttributes.add(key);
                }
                break;
            default:
                if (userMetadata[attribute]) {
                    customAttributes.add(attribute);
                }
        }
    }

    for (const key of customAttributes) {
        xml.push(`<${key}>${userMetadata[key]}</${key}>`);
    }
}

module.exports = {
    parseAttributesHeaders,
    buildAttributesXml,
};
