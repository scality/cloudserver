const async = require('async');
const { parseString } = require('xml2js');
const { receiveData, isSystemXML, getFileToBuild } = require('./utils');
const { s3routes, errors } = require('arsenal');
const metadata = require('../../metadata/wrapper');
const parseSystemSchema = require('./schemas/system');
const parseCapacitySchema = require('./schemas/capacity');
const writeContinue = require('../../utilities/writeContinue');

const { responseNoBody, responseXMLBody } = s3routes.routesUtils;

/**
 * Puts a veeam capacity or system file in the bucket metadata.
 * Logic ensures consistency of the data and metadata.
 *
 * @param {object} request - request object
 * @param {object} response - response object
 * @param {object} bucketMd - bucket metadata from the db
 * @param {object} log - logger object
 * @returns {undefined} -
 */
function putVeeamFile(request, response, bucketMd, log) {
    if (!bucketMd) {
        return errors.NoSuchBucket;
    }

    return async.waterfall([
        next => {
            // Extract the data from the request, keep it in memory
            writeContinue(request, response);
            return receiveData(request, log, next);
        },
        (value, next) => parseString(value, { explicitArray: false }, (err, parsed) => {
            // Convert the received XML to a JS object
            if (err) {
                return next(errors.MalformedXML);
            }
            return next(null, parsed);
        }),
        (parsedXML, next) => {
            const capabilities = bucketMd._capabilities || {
                VeeamSOSApi: {},
            };
            // Validate the JS object schema with joi and prepare the object for
            // further logic
            const validateFn = isSystemXML(request.objectKey) ? parseSystemSchema : parseCapacitySchema;
            let validatedData = null;
            try {
                validatedData = validateFn(parsedXML);
            } catch (err) {
                log.error('xml file did not pass validation', { err });
                return next(errors.MalformedXML);
            }
            const file = getFileToBuild(request, validatedData, true);
            if (file.error) {
                return next(file.error);
            }
            capabilities.VeeamSOSApi = {
                ...(capabilities.VeeamSOSApi || {}),
                ...file.value,
            };
            // Write data to bucketMD with the same (validated) format
            // eslint-disable-next-line no-param-reassign
            bucketMd = {
                ...bucketMd,
                _capabilities: capabilities,
            };
            // Update bucket metadata
            return metadata.updateBucket(request.bucketName, bucketMd, log, next);
        }
    ], err => {
        if (err) {
            return responseXMLBody(err, null, response, log);
        }
        return responseNoBody(null, null, response, 200, log);
    });
}

module.exports = putVeeamFile;
