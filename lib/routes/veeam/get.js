const xml2js = require('xml2js');
const { errors } = require('arsenal');
const metadata = require('../../metadata/wrapper');
const { respondWithData, buildHeadXML, getFileToBuild, isSystemXML, fetchCapacityMetrics } = require('./utils');
const { responseXMLBody } = require('arsenal/build/lib/s3routes/routesUtils');

/**
 * Returns system.xml or capacity.xml files for a given bucket.
 *
 * @param {object} request - request object
 * @param {object} response - response object
 * @param {object} bucketMd - bucket metadata from the db
 * @param {object} log - logger object
 * @returns {undefined} -
 */
function getVeeamFile(request, response, bucketMd, log) {
    if (!bucketMd) {
        return responseXMLBody(errors.NoSuchBucket, null, response, log);
    }
    if ('tagging' in request.query) {
        return respondWithData(request, response, log, bucketMd,
            buildHeadXML('<Tagging><TagSet></TagSet></Tagging>'));
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

            // Extract the last modified date, but do not include it when computing
            // the file's ETag (md5)
            const modified = bucketMetrics?.date || new Date();
            delete fileToBuild.value.LastModified;

            // The SOSAPI metrics are dynamically computed using the SUR backend.
            if (bucketMetrics && !fileToBuild.value.CapacityInfo?.Used) {
                fileToBuild.value.CapacityInfo.Used = Number(bucketMetrics.bytesTotal);
                fileToBuild.value.CapacityInfo.Available =
                    Number(fileToBuild.value.CapacityInfo.Capacity) - Number(bucketMetrics.bytesTotal);
                // TODO CLDSRV-633 when SUR backend supports realtime metrics: it will
                // report the real last cseq/date processed by SUR, instead of the current date,
                // ensuring no issue in a SOSAPI context. We should use this information.
            }

            const builder = new xml2js.Builder({
                headless: true,
            });
            return respondWithData(request, response, log, data,
                buildHeadXML(builder.buildObject(fileToBuild.value)), modified);
        };

        if (!isSystemXML(request.objectKey)) {
            return fetchCapacityMetrics(bucketMd, request, log, 'getVeeamFile', (err, bucketMetrics) => {
                if (err) {
                    return responseXMLBody(errors.InternalError, null, response, log);
                }
                return finalizeRequest(bucketMetrics);
            });
        }
        return finalizeRequest();
    });
}

module.exports = getVeeamFile;
