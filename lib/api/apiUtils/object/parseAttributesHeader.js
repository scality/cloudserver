const { errorInstances } = require('arsenal');
const { supportedGetObjectAttributes } = require('../../../../constants');

/**
 * parseAttributesHeaders - Parse and validate the x-amz-object-attributes header
 * @param {object} headers - request headers
 * @returns {string[]} - array of valid attribute names
 * @throws {Error} - InvalidRequest if header is missing/empty, InvalidArgument if attribute is invalid
 */
function parseAttributesHeaders(headers) {
  const attributes = headers['x-amz-object-attributes']?.split(',').map(attr => attr.trim()) ?? [];
  if (attributes.length === 0) {
    throw errorInstances.InvalidRequest.customizeDescription(
      'The x-amz-object-attributes header specifying the attributes to be retrieved is either missing or empty',
    );
  }

  if (attributes.some(attr => !supportedGetObjectAttributes.has(attr))) {
    throw errorInstances.InvalidArgument.customizeDescription('Invalid attribute name specified.');
  }

  return attributes;
}

module.exports = parseAttributesHeaders;
