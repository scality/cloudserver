import { errors } from 'arsenal';
import { parseString } from 'xml2js';

const tagRegex = new RegExp(/[^a-zA-Z0-9 +-=._:/]/g);

/*
    Format of xml request:

    <Tagging>
      <TagSet>
         <Tag>
           <Key>Tag Name</Key>
           <Value>Tag Value</Value>
         </Tag>
      </TagSet>
    </Tagging>
*/


export const _validator = {
    validateTagStructure: tag => tag
        && Object.keys(tag).length === 2
        && tag.Key && tag.Value
        && tag.Key.length === 1 && tag.Value.length === 1
        && tag.Key[0] && tag.Value[0]
        && typeof tag.Key[0] === 'string' && typeof tag.Value[0] === 'string',

    validateXMLStructure: result =>
        result && Object.keys(result).length === 1 &&
        result.Tagging &&
        result.Tagging.TagSet &&
        result.Tagging.TagSet.length === 1 &&
        (
          result.Tagging.TagSet[0] === '' ||
          result.Tagging.TagSet[0] &&
          Object.keys(result.Tagging.TagSet[0]).length === 1 &&
          result.Tagging.TagSet[0].Tag &&
          Array.isArray(result.Tagging.TagSet[0].Tag)
        ),

    validateKeyValue: (key, value) => {
        if (key.length > 128 || key.match(tagRegex)) {
            return errors.InvalidTag.customizeDescription('The TagKey you ' +
              'have provided is invalid');
        }
        if (value.length > 256 || value.match(tagRegex)) {
            return errors.InvalidTag.customizeDescription('The TagValue you ' +
              'have provided is invalid');
        }
        return true;
    },
};
/** _validateTags - Validate tags, returning an error if tags are invalid
* @param {object[]} tags - tags parsed from xml to be validated
* @param {string[]} tags[].Key - Name of the tag
* @param {string[]} tags[].Value - Value of the tag
* @return {(Error|object)} tagsResult - return object tags on success
* { key: value}; error on failure
*/
function _validateTags(tags) {
    let result;
    const tagsResult = {};

    if (tags.length === 0) {
        return tagsResult;
    }
    // Maximum number of tags per resource: 10
    if (tags.length > 10) {
        return errors.BadRequest
            .customizeDescription('Object tags cannot be greater than 10');
    }
    for (let i = 0; i < tags.length; i++) {
        const tag = tags[i];

        if (!_validator.validateTagStructure(tag)) {
            return errors.MalformedXML;
        }
        const key = tag.Key[0];
        const value = tag.Value[0];

        // Allowed characters are letters, whitespace, and numbers, plus
        // the following special characters: + - = . _ : /
        // Maximum key length: 128 Unicode characters
        // Maximum value length: 256 Unicode characters
        result = _validator.validateKeyValue(key, value);
        if (result instanceof Error) {
            return result;
        }

        tagsResult[key] = value;
    }
    // not repeating keys
    if (tags.length > Object.keys(tagsResult).length) {
        return errors.InvalidTag.customizeDescription('Cannot provide ' +
        'multiple Tags with the same key');
    }
    return tagsResult;
}

/** parseTagXml - Parse and validate xml body, returning callback with object
* tags : { key: value}
* @param {string} xml - xml body to parse and validate
* @param {object} log - Werelogs logger
* @param {function} cb - callback to server
* @return {undefined} - calls callback with tags object on success, error on
*   failure
*/
export function parseTagXml(xml, log, cb) {
    parseString(xml, (err, result) => {
        if (err) {
            log.trace('xml parsing failed', {
                error: err,
                method: 'parseTagXml',
            });
            log.debug('invalid xml', { xml });
            return cb(errors.MalformedXML);
        }
        if (!_validator.validateXMLStructure(result)) {
            log.debug('xml validation failed', {
                error: errors.MalformedXML,
                method: '_validator.validateXMLStructure',
                xml,
            });
            return cb(errors.MalformedXML);
        }
        // AWS does not return error if no tag
        if (result.Tagging.TagSet[0] === '') {
            return cb(null, []);
        }
        const validationRes = _validateTags(result.Tagging.TagSet[0].Tag);
        if (validationRes instanceof Error) {
            log.debug('tag validation failed', {
                error: validationRes,
                method: '_validateTags',
                xml,
            });
            return cb(validationRes);
        }
        // if no error, validation returns tags object
        return cb(null, validationRes);
    });
}
