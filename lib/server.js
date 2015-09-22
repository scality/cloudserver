/// <reference path="../typings/node/node.d.ts"/>
var fs = require('fs');
var jsutils = require("jsutils");
var cluster = require('cluster');
var Bucket = require('./Bucket');
var checkAuth = require("./auth/checkAuth").checkAuth;
var getBucketsByUser = require('./REST_API/getBucketsByUser.js');
var async = require('async');

//var profiler = require('v8-profiler');

var CONFIG = {
    CLUSTERING: true,
    CLUSTERING_FORKS: 10
};

if (CONFIG.CLUSTERING && cluster.isMaster) {
    for (var n=0; n<CONFIG.CLUSTERING_FORKS; n++)
        cluster.fork();

    cluster.on('disconnect', function (worker) {
        console.error('worker disconnected, start a new one');
        cluster.fork();
    });
}
else {
  (function () {
      var Router, argv, http, router, server, clean_up;
      var logs;

      logs = new jsutils.LogCtx("ironman", "info", false, CONFIG.LOGDIR);

      var domain = require('domain');

      Router = require('node-simple-router');


      http = require('http');


      /*
       * Tune HTTP client behavior
      */
      //http.globalAgent.maxSockets = CONFIG.FANOUT_CLIENT_POOL_MAX_SOCKETS_PER_HOST;
      http.globalAgent.keepAlive = true;
      //http.globalAgent.maxFreeSockets = CONFIG.FANOUT_CLIENT_POOL_MAX_FREE_SOCKETS;

      router = Router({
          list_dir: true
      });

      router.get("/", function(request, response){
        console.log("getting")
        console.log("request in slash", request);
        checkAuth(request, response, function(err, accessKey){
          if(err){
            return _error_response(response, "Authorization Failed" + err, 403);
          } 
            getBucketsByUser(accessKey, response, function(err, stuff){
              if(err){
                return _error_response(response, err, code);
              }
              return _ok_response(response, stuff);
          });
        })
      });

      //Changed to "/:whatever" from "/answertoall" to test with s3cmd
      router.get("/:whatever", function(request, response) {
        console.log(request)
        checkAuth(request, function(err, accessKey) {
            if(err) return _error_response(response, "Authorization Failed" + err, 403);

            bucket = new Bucket();

            var testObjects = [
              {"key1": "value1"}, {"key2": "value2"}, {"key2/plop1": "value2/plop1"}, {"key2/plop2": "value2/plop2"}, {"key3": "value3"}, {"key4/": "plop"}
            ];
            async.each(testObjects, function(obj, cb) {
              var key = Object.keys(obj)[0];
              var value = obj[key];
              bucket.PUTObject(key, value, function() {
                cb();
              });
            }, function(err) {
              bucket.GETBucketListObjects('key', null, '/', 5, function(msg){
                return _ok_response(response, msg);
              });
            });
          });
      });

      //Route to test auth
      router.put("/:bucket/:obj", function(request, response){
           checkAuth(request, function(err, accessKey){
          if(err) return _error_response(response, "Authorization Failed" + err, 403);

        });
      });

      var _error_response = function (response, msg, code) {
          code = code || 500;

          response.writeHead(code, {
              'Content-type': 'text/javascript'
          });
          return response.end(JSON.stringify({
              error: msg
          }, null, 4));
      };

      var _ok_response = function (response, msg) {
          response.writeHead(200, {
              'Content-type': 'text/javascript'
          });
          return response.end(JSON.stringify({
            msg: msg,
          }, null, 4));
        };

      argv = process.argv.slice(2);

      var http_server_responder = function (request, response) {
          var d = domain.create();

          var recover =  function (er) {
              console.error("domain", 'error', er.stack);
              // Note: we're in dangerous territory!
              // By definition, something unexpected occurred,
              // which we probably didn't want.
              // Anything can happen now!  Be very careful!
              try {
                  // stop taking new requests.
                  if (CONFIG.CLUSTERING) {
                      // make sure we close down within 30 seconds
                      var killtimer = setTimeout(function() {
                        process.exit(1);
                      }, 5000);
                      // But don't keep the process open just for that!
                      killtimer.unref();

                      server.close();
                      // Let the master know we're dead.  This will trigger a
                      // 'disconnect' in the cluster master, and then it will fork
                      // a new worker.
                      cluster.worker.disconnect();
                  }

                  // try to send an error to the request that triggered the problem
                  response.statusCode = 500;
                  response.setHeader('content-type', 'text/plain');
                  response.end('Oops, there was a problem!\n' + er.toString());
              } catch (er2) {
                  // oh well, not much we can do at this point.
                  console.error('Error sending 500!', er2.stack);
              }
          };

          d.on('error', recover);

          d.run(function () {
              router(request, response);
          });
      };

      var server = http.createServer(http_server_responder);
      server.setTimeout(CONFIG.HTTP_SERVER_TIMEOUT_MS);

      server.on('listening', function () {
          var addr;
          addr = server.address() || {
              address: '0.0.0.0',
              port: argv[0] || 8000
          };
          router.log("Serving web content at " + addr.address + ":" + addr.port + " - PID: " + process.pid);
      });

      clean_up = function () {
          router.log(" ");
          router.log("Server shutting down...");
          router.log(" ");
          server.close();
          return process.exit(0);
      };

      process.on('SIGINT', clean_up);
      process.on('SIGHUP', clean_up);
      process.on('SIGQUIT', clean_up);
      process.on('SIGTERM', clean_up);
      process.on('SIGPIPE', function() {});

      server.listen((argv[0] != null) && !isNaN(parseInt(argv[0])) ? parseInt(argv[0]) : 8000);

  }).call(this);
}