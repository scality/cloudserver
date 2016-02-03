import constants from '../../constants';
import services from '../services';
import utils from '../utils';

const splitter = constants.splitter;

/*
 *  Format of xml response:
 *
 *  <?xml version="1.0" encoding="UTF-8"?>
 *  <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
 *  <Owner>
 *  <ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
 *  <DisplayName>webfile</DisplayName>
 *  </Owner>
 *  <Buckets>
 *  <Bucket>
 *  <Name>quotes</Name>
 *  <CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
 *  </Bucket>
 *  <Bucket>
 *  <Name>samples</Name>
 *  <CreationDate>2006-02-03T16:41:58.000Z</CreationDate>
 *  </Bucket>
 *  </Buckets>
 *  </ListAllMyBucketsResult>
 */


 /*
    Construct JSON in proper format to be converted to XML
    to be returned to client
 */
function _constructJSON(infoToConvert) {
    const { userBuckets, authInfo } = infoToConvert;
    const buckets = userBuckets.map((userBucket) => {
        return {
            Bucket: [
                { Name: userBucket.key.split(splitter)[1] },
                { CreationDate: userBucket.value.creationDate },
            ]
        };
    });

    return {
        ListAllMyBucketsResult: [
            {
                _attr: { xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/' },
            },
            {
                Owner: [
                    { ID: authInfo.getCanonicalID() },
                    { DisplayName: authInfo.getAccountDisplayName() },
                ],
            },
            { Buckets: buckets },
        ]};
}

/**
 * GET Service - Get list of buckets owned by user
 * @param  {AuthInfo} Instance of AuthInfo class with requester's info
 * @param {object} request - normalized request object
 * @param  {object} log - Werelogs logger
 * @return {function} callback with error, object data
 * result and responseMetaHeaders as arguments
 */
export default function serviceGet(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketDelete' });

    if (authInfo.isRequesterPublicUser()) {
        log.warn('operation not available for public user');
        return callback('AccessDenied');
    }
    services.getService(authInfo, request, log, (err, userBuckets) => {
        if (err) {
            return callback(err);
        }
        const infoToConvert = {
            userBuckets,
            authInfo,
        };
        const xml = utils.convertToXml(infoToConvert, _constructJSON);
        return callback(null, xml);
    });
}
