import services from '../services.js';
import xml from "xml";



/**
 * GET Service - Get list of buckets owned by user
 * @param  {string} accessKey - user's access key
 * @param {object} metastore - metastore with buckets
 * containing objects and their metadata
 * @param {object} request - normalized request object
 * @return {function} callback with error, object data
 * result and responseMetaHeaders as arguments
 */


/*
   Format of xml response:

   <?xml version="1.0" encoding="UTF-8"?>
   <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
   <Owner>
   <ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
   <DisplayName>webfile</DisplayName>
   </Owner>
   <Buckets>
   <Bucket>
   <Name>quotes</Name>
   <CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
   </Bucket>
   <Bucket>
   <Name>samples</Name>
   <CreationDate>2006-02-03T16:41:58.000Z</CreationDate>
   </Bucket>
   </Buckets>
   </ListAllMyBucketsResult>*/


function _constructJSON(userBuckets, request, accessKey) {
    const date = new Date();
    let month = (date.getMonth() + 1).toString();
    if (month.length === 1) {
        month = `0${month}`;
    }

    const dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;
    const hostname = request.lowerCaseHeaders.host.split(":")[0];

    const buckets = [];

    for (let i = 0, ii = userBuckets.length; i < ii; i++) {
        const bucket = {
            "Bucket": [
                {
                    "Name": userBuckets[i].name
                },
                {
                    "CreationDate": userBuckets[i].creationDate.toISOString()
                }
            ]
        };
        buckets.push(bucket);
    }


    const json = {
        "ListAllMyBucketsResult": [
            {
                _attr: {
                    "xmlns": `http:\/\/${hostname}/doc/${dateString}`
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

    return json;
}

function _convertToXml(data, request, accessKey) {
    const constructedJSON = _constructJSON(data, request, accessKey);
    const constructedXml =
    xml(constructedJSON, { declaration: { encoding: 'UTF-8' }});
    return constructedXml;
}

export default function serviceGet(accessKey, metastore, request, callback) {
    services.getService(accessKey, metastore, request, function (err, result) {
        if (err) {
            return callback(err);
        }
        const xml = _convertToXml(result, request, accessKey);
        return callback(null, xml);
    });
}
