const { errorInstances } = require('arsenal');
const { supportedGetObjectAttributes } = require('../../../../constants');

/**
 * Parse and validate attribute headers from a request.
 * @param {object} headers - Request headers object
 * @returns {Set<string>} - set of requested attribute names
 * @throws {arsenal.errors.InvalidRequest} When header is required but missing/empty
 * @throws {arsenal.errors.InvalidArgument} When an invalid attribute name is specified
 */
function parseAttributesHeaders(headers) {
  const attributes = headers['x-amz-object-attributes']?.split(',').map(attr => attr.trim()) ?? [];
  if (attributes.length === 0) {
    throw errorInstances.InvalidRequest.customizeDescription(
      'The x-amz-object-attributes header specifying the attributes to be retrieved is either missing or empty',
    );
  }

  if (attributes.some(attr => !attr.startsWith('x-amz-meta-') && !supportedGetObjectAttributes.has(attr))) {
    throw errorInstances.InvalidArgument.customizeDescription('Invalid attribute name specified.');
  }

  return new Set(attributes);
}

module.exports = parseAttributesHeaders;
