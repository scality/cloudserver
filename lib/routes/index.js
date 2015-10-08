'use strict';

module.exports = function(router) {
  var Bucket = require('../bucket_mem.js');
  var async = require('async');
  var utils = require('../utils.js');
  var checkAuth = require("../auth/checkAuth.js").checkAuth;
  var serviceGet = require('../api/serviceGet.js');
  var bucketHead = require('../api/bucketHead.js');
  var bucketGet = require('../api/bucketGet.js');
  var bucketPut = require('../api/bucketPut.js');
  var bucketDelete = require('../api/bucketDelete.js');
  var objectPut = require('../api/objectPut.js');
  var objectGet = require('../api/objectGet.js');
  var objectHead = require('../api/objectHead.js');
  var objectDelete = require('../api/objectDelete.js');

  let datastore = {};
  let metastore = require('../testdata/metadata.json');

  var _ok_header_response = function (response, code) {
   code = code || 500;
   response.writeHead(code);
   return response.end(function(){ console.log('response ended')});
  };

  var _error_header_response = function(response, code) {
    code = code || 500;
    response.writeHead(code);
    return response.end(function(){ console.log('response ended') });
  }

  var _error_response = function (response, msg, code) {
     code = code || 500;

     response.writeHead(code, {
         'Content-type': 'text/javascript'
     });
     return response.end(JSON.stringify({
         error: msg
     }, null, 4));
  };

  var _ok_json_response = function (response, msg) {
     response.writeHead(200, {
         'Content-type': 'application/json'
     });
     return response.end(JSON.stringify({
       msg: msg,
     }, null));
  };

  var _ok_xml_response = function (response, xml) {
     response.writeHead(200, {
        'Content-type': 'application/xml'
     });
     return response.end(xml, 'utf8');
  };

  var _error_xml_response = function (response, xml, code) {
    code = code || 500;
    response.writeHead(code, {
      'Content-type': 'application/xml'
    });
    return response.end(xml, 'utf8');
  };

  router.get("/(.*)", function(request, response){
    request = utils.normalizeRequest(request);
    checkAuth(request, function(err, accessKey){
      if(err){
        return _error_response(response, "Authorization Failed: " + err, 403);
      }

      let resourceRes = utils.getResourceNames(request);
      let bucketname = resourceRes.bucket;
      let objectKey = resourceRes.object;

      if(bucketname == undefined && objectKey === undefined) {
        // GET service
        serviceGet(accessKey, metastore, request, function(err, xml) {
          if(err) {
            return _error_response(response, err);
          }
          return _ok_xml_response(response, xml);
        });
      } else if(objectKey === undefined) {
        // GET bucket
        bucketGet(accessKey, metastore, request, function(err, xml){
          if(err){
            return _error_response(response, err);
          }
          return _ok_xml_response(response, xml);
        });
      } else {
        // GET object
        objectGet(accessKey, datastore,  metastore, request, function(err, result, responseMetaHeaders){
            if(err){
              let errorXmlRes = utils.buildResponseErrorXML(err);
              return _error_xml_response(response, errorXmlRes.xml, errorXmlRes.httpCode);
            }

            response = utils.buildGetSuccessfulResponse(request.lowerCaseHeaders, response, responseMetaHeaders);
            return response.end(result);
        });
      }
    });
  });

  //Put bucket
  router.put('/(.*)', function(request, response) {
    request = utils.normalizeRequest(request);
    checkAuth(request, function(err, accessKey){
      if(err) {
        return _error_response(response, "Authorization Failed: " + err, 403);
      }
      let resourceRes = utils.getResourceNames(request);
      let bucketname = resourceRes.bucket;
      let objectKey = resourceRes.object;
      return _ok_xml_response(response, null);
    });
  });


  /**
  * PUT resource - supports both bucket and object
  * If bucket name is in hostname then the PUT is for creating the object in the bucket
  * or else the PUT is for creating a new bucket
  * @param {string} path style - It can be /<bucket name> or /<object name>
  * @param {function} callback - Callback to be called upon completion
  */
  router.put('/:resource', function(request, response) {
    request = utils.normalizeRequest(request);

     checkAuth(request, function(err, accessKey){

       if(err) {
         return _error_response(response, "Authorization Failed: " + err, 403);
       }

       let bucketname = utils.getBucketNameFromHost(request);

       /* If bucket name is not in hostname, create a new bucket */
       if(bucketname === undefined) {
         bucketPut(accessKey, metastore, request, function(err, result) {
           if(err) {
             return _error_response(response, err, 500); //error code tbd
           }
           return _ok_header_response(response, 200);
         });
       }

       /* NEED to test this: (not sure if request body will occur as a stream or a separate request) */
       if (request.headers.expect && request.headers.expect === '100-continue') {
         response.writeHead(100);
       }

       /* Create object if bucket name is in the hostname */
       if(bucketname){
        objectPut(accessKey, datastore,  metastore, request, function(err, result){
            if(err){
              return _error_response(response, err);
            }
            return _ok_header_response(response, 200);
        });
       }
    });
  });

  //Put object in bucket where bucket is named in host or path
  router.put('/:resource/(.*)', function(request, response) {
    request = utils.normalizeRequest(request);
    checkAuth(request, function(err, accessKey) {
     if(err){
       return _error_response(response, "Authorization Failed: " + err, 403);
     }

     //Put object using bucket name in path
     if(utils.getBucketNameFromHost(request) === undefined){
        objectPut(accessKey, datastore, metastore, request, function(err, result){
          if(err){
            return _error_response(response, err);
          }
          response = utils.buildResponseHeaders(response, {'ETag': result})
          return _ok_header_response(response, 200);
        });
     }

     //Put object using bucket name in host
     if(utils.getBucketNameFromHost(request)){
      objectPut(accessKey, datastore, metastore, request, function(err, result){
         if(err){
           return _error_response(response, err);
         }
         console.log('Object PUT succeeded!');
         return _ok_header_response(response, 200);
      });
     }
    });
  });

  /**
   * DELETE resource - deletes bucket or object
   * @param {string} path style - It can be /<bucket name> or /<object name>
   * @param {function} callback with request and response objects
   * @return {object} error or success response
   */

  router.delete('/(.*)', function(request, response) {
    request = utils.normalizeRequest(request);
    checkAuth(request, function(err, accessKey){
      if(err) {
        return _error_response(response, "Authorization Failed: " + err, 403);
      }

      let resourceRes = utils.getResourceNames(request);
      let bucketname = resourceRes.bucket;
      let objectKey = resourceRes.object;

      if(objectKey === undefined) {
        //delete bucket
        bucketDelete(accessKey, metastore, request, function(err, result, responseHeaders) {
          if(err) {
            let errorXmlRes = utils.buildResponseErrorXML(err);
            return _error_xml_response(response, errorXmlRes.xml, errorXmlRes.httpCode);
          }
          response = utils.buildResponseHeaders(response, responseHeaders.headers);
          return _ok_header_response(response, 204);
        });

      } else {
        //delete object
        objectDelete(accessKey, datastore, metastore, request, function(err, result, responseHeaders) {
          if(err) {
            return _error_response(response, err);
          }
          response = utils.buildResponseHeaders(response, responseHeaders);
          return _ok_header_response(response, 204);
        });
      }
    });
  });

   router.head("/", function(request, response){
    request = utils.normalizeRequest(request);
     checkAuth(request, function(err, accessKey){
       if(err){
         return _error_response(response, "Authorization Failed: " + err, 403);
       }

       // If bucket name in host, HEAD Bucket
       if(utils.getBucketNameFromHost(request) !== undefined){
          bucketHead(accessKey, metastore, request, function(err, success){
            if(err){
              return _error_response(response, err);
            }
            return _ok_header_response(response, 200);
          });
       }

       //No route for "any" without a bucket name in host.
      return _error_response(response, "Invalid request");

     });
   });



   router.head("/:resource", function(request, response){
     request = utils.normalizeRequest(request);
     checkAuth(request, function(err, accessKey){

       if(err){
         return _error_response(response, "Authorization Failed: " + err, 403);
       }


       //HEAD Bucket using bucket name in path
       if(utils.getBucketNameFromHost(request) === undefined){
          bucketHead(accessKey, metastore, request, function(err, success){
            if(err){
              return _error_response(response, err);
            }
            return _ok_header_response(response, 200);
          });
       }

       //HEAD Object using bucket name in host

    });
   });


   router.head("/:resource/(.*)", function(request, response){
     request = utils.normalizeRequest(request);
     checkAuth(request, function(err, accessKey){
       if(err){
         return _error_response(response, "Authorization Failed: " + err, 403);
       }
       //HEAD Object using bucket name in path
       // or
       //HEAD Object using buckent name in host (meaning object = :resource/:furtherObjectName)

       objectHead(accessKey, metastore, request, function(err, responseMetaHeaders){
           if(err){
             return _error_response(response, err, 404);
           }
           response = utils.buildGetSuccessfulResponse(request.lowerCaseHeaders, response, responseMetaHeaders);
           return response.end(function(){ console.log('response ended')});
       });




     });
   });
}
