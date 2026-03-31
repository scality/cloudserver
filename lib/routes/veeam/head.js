const xml2js = require('xml2js');
const { errors } = require('arsenal');
const metadata = require('../../metadata/wrapper');
const { getResponseHeader, buildHeadXML, getFileToBuild, isSystemXML } = require('./utils');
const { responseXMLBody, responseContentHeaders } = require('arsenal/build/lib/s3routes/routesUtils');
const UtilizationService = require('../../../lib/utilization/instance');

/**
 * Returns system.xml or capacity.xml files metadata for a given bucket.
 *
 * @param {object} request - request object
 * @param {object} response - response object
 * @param {object} bucketMd - bucket metadata from the db
 * @param {object} log - logger object
 * @returns {undefined} -
 */
function headVeeamFile(request, response, bucketMd, log) {
    if (!bucketMd) {
        return responseXMLBody(errors.NoSuchBucket, null, response, log);
    }

    return metadata.getBucket(request.bucketName, log, (err, data) => {
        if (err) {
            return responseXMLBody(errors.InternalError, null, response, log);
        }

        const finalizeRequest = bucketMetrics => {
            const fileToBuild = getFileToBuild(request, data._capabilities?.VeeamSOSApi);
            if (fileToBuild.error) {
                return responseXMLBody(fileToBuild.error, null, response, log);
            }

            // Extract the last modified date, but do not include it when computing the file's ETag (md5)
            const modified = bucketMetrics?.date || new Date();
            delete fileToBuild.value.LastModified;
            const builder = new xml2js.Builder({
                headless: true,
            });
            const dataBuffer = Buffer.from(buildHeadXML(builder.buildObject(fileToBuild)));

            return responseContentHeaders(
                null,
                {},
                getResponseHeader(request, data, dataBuffer, modified, log),
                response,
                log,
            );
        };

        if (!isSystemXML(request.objectKey)) {
            const bucketKey = `${bucketMd._name}_${new Date(bucketMd._creationDate).getTime()}`;

            return UtilizationService.getUtilizationMetrics(
                'bucket', 
                bucketKey, 
                null, 
                {}, 
                (utilizationErr, bucketMetrics) => {
                    if (utilizationErr) {
                        const statusCode = utilizationErr.response?.status || 
                            utilizationErr.statusCode ||
                            utilizationErr.code;

                        if (statusCode === 404) {
                            log.warn('UtilizationService returned 404 when fetching capacity metrics', {
                                method: 'headVeeamFile',
                                bucket: request.bucketName,
                                error: utilizationErr.message || utilizationErr.code,
                            });
                            return finalizeRequest();
                        }

                        log.error('error fetching capacity metrics from UtilizationService', {
                            method: 'headVeeamFile',
                            bucket: request.bucketName,
                            error: utilizationErr.message || utilizationErr.code,
                            statusCode,
                        });

                        return responseXMLBody(errors.InternalError, null, response, log);
                    }

                    return finalizeRequest(bucketMetrics);
                },
            );
        }

        return finalizeRequest();
    });
}

module.exports = headVeeamFile;
