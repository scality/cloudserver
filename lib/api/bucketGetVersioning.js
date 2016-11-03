import services from '../services';

//	Sample XML response:
/*
   <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Status>VersioningState</Status>
       <MfaDelete>MfaDeleteState</MfaDelete>
   </VersioningConfiguration>
*/

/**
 * Convert Versioning Configuration object of a bucket into xml format.
 * @param {object} versioningConfiguration - versioning configuration object
 * @return {string} - the converted xml string of the versioning configuration
 */
function convertToXml(versioningConfiguration) {
    const xml = [];

    xml.push('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
             '<VersioningConfiguration ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    );

    if (versioningConfiguration && versioningConfiguration.Status) {
        xml.push(`<Status>${versioningConfiguration.Status}</Status>`);
    }

    if (versioningConfiguration && versioningConfiguration.MfaDelete) {
        xml.push(`<MfaDelete>${versioningConfiguration.MfaDelete}</MfaDelete>`);
    }

    xml.push('</VersioningConfiguration>');

    return xml.join('');
}

/**
 * bucketGetVersioning - Return Versioning Configuration for bucket
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
 */
export default function bucketGetVersioning(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetVersioning' });

    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGetVersioning',
        log,
    };

    services.metadataValidateAuthorization(metadataValParams, (err, bucket) => {
        if (err) {
            log.debug('error processing request',
                { method: 'bucketGetVersioning', error: err });
            return callback(err);
        }
        const versioningConfiguration = bucket.getVersioningConfiguration();
        const xml = convertToXml(versioningConfiguration);
        return callback(null, xml);
    });
}
