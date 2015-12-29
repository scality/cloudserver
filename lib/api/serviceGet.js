import Config from '../Config';
import services from '../services';
import utils from '../utils.js';

const config = new Config();
const splitter = config.splitter;

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


function _constructJSON(infoToConvert) {
    const { userBuckets, request, accessKey } = infoToConvert;
    const date = new Date();
    let month = (date.getMonth() + 1).toString();
    if (month.length === 1) {
        month = `0${month}`;
    }

    const dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;
    const hostname = utils.getResourceNames(request).host;
    const buckets = userBuckets.map((userBucket) => {
        return {
            "Bucket": [
                {
                    "Name": userBucket.key.split(splitter)[1]
                },
                {
                    "CreationDate": userBucket.value.creationDate
                }
            ]
        };
    });

    return {
        "ListAllMyBucketsResult": [
            {
                _attr: {
                    "xmlns": `http://${hostname}/doc/${dateString}`
                }
            },
            {
                "Owner": [
                    {
                        "ID": accessKey
                    },
                    {
                        "DisplayName": accessKey
                    }
                ]
            },
            {
                "Buckets": buckets
            }
        ]};
}


/**
 * GET Service - Get list of buckets owned by user
 * @param  {string} accessKey - user's access key
 * @param {object} metastore - metastore with buckets
 * containing objects and their metadata
 * @param {object} request - normalized request object
 * @param  {function} log - Werelogs logger
 * @return {function} callback with error, object data
 * result and responseMetaHeaders as arguments
 */
export default function serviceGet(accessKey, metastore, request, log,
    callback) {
    if (accessKey === 'http://acs.amazonaws.com/groups/global/AllUsers') {
        return callback('AccessDenied');
    }
    services.getService(accessKey, request, (err, userBuckets) => {
        if (err) {
            return callback(err);
        }
        const infoToConvert = {
            userBuckets,
            request,
            accessKey
        };
        const xml = utils.convertToXml(infoToConvert, _constructJSON);
        return callback(null, xml);
    });
}
