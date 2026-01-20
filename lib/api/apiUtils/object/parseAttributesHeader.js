const { errorInstances } = require('arsenal');
const { allowedObjectAttributes } = require('../../../../constants');

/**
 * parseAttributesHeaders - Parse and validate the x-amz-object-attributes header
 * @param {object} headers - request headers
 * @returns {string[]} - array of valid attribute names
 * @throws {Error} - InvalidRequest if header is missing/empty, InvalidArgument if attribute is invalid
 */
function parseAttributesHeaders(headers) {
  const raw = headers['x-amz-object-attributes'] || '';

  const attributes = raw
    .split(',')
    .map(s => s.trim())
    .filter(s => s !== '');

  if (attributes.length === 0) {
    throw errorInstances.InvalidRequest.customizeDescription(
      'The x-amz-object-attributes header specifying the attributes to be retrieved is either missing or empty',
    );
  }

  const invalids = attributes.filter(s => !allowedObjectAttributes.has(s));
  if (invalids.length > 0) {
    throw errorInstances.InvalidArgument.customizeDescription('Invalid attribute name specified.');
  }

  return attributes;
}

module.exports = parseAttributesHeaders;
