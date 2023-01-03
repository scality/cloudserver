const joi = require('joi');
const { errors } = require('arsenal');

/**
 * Validates and parse the provided JSON object from the
 * provided XML file. XML scheme example:
 *
 * <?xml version="1.0" encoding="utf-8" ?>
 * <CapacityInfo>
 *   <Capacity>1099511627776</Capacity>
 *   <Available>0</Available>
 *   <Used>0</Used>
 * </CapacityInfo>
 *
 * @param {string} parsedXML - the parsed XML from xml2js
 * @returns {object | Error} the valid system.xml JS object or an error if
 * validation fails
 */
function validateCapacitySchema(parsedXML) {
    const schema = joi.object({
        CapacityInfo: joi.object({
            Capacity: joi.number().min(-1).required(),
            Available: joi.number().min(-1).required(),
            Used: joi.number().min(-1).required(),
        }).required(),
    });
    const validatedData = schema.validate(parsedXML, {
        // Allow any unknown keys for future compatibility
        allowUnknown: true,
        convert: true,
    });
    if (validatedData.error) {
        throw new Error(errors.MalformedXML);
    }
    return validatedData.value;
}

module.exports = validateCapacitySchema;
