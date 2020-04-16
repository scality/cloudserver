const { S3 } = require('aws-sdk');
const config = {
    sslEnabled: false,
    endpoint: 'http://127.0.0.1:8000', // or 'localhost:8000'
    signatureCache: false,
    signatureVersion: 'v4',
    region: 'us-east-1',
    s3ForcePathStyle: true,
    accessKeyId: 'accessKey1',
    secretAccessKey: 'verySecretKey1',
};

const s3 = new S3(config);

console.log(`\nObject.values(s3): ,${Object.values(JSON.stringify(s3))}\n`);

s3.listBuckets(function(err, data) {
  if (err) console.log(err, err.stack);
  else     console.log(`LN:[19]\n ${JSON.stringify(data)}`);
});

/* createBucket */
 var bucket1 = {
  Bucket: "felix-test-bucket"
 };
 var bucket2 = {
  Bucket: "ilke-test-bucket"
 };
s3.createBucket(bucket1, function(err, data) {
   if (err) console.log(err, err.stack);
   else     console.log(`LN:[31]\n ${JSON.stringify(data)}`);
});
s3.createBucket(bucket2, function(err, data) {
   if (err) console.log(err, err.stack);
   else     console.log(`LN:[35]\n ${JSON.stringify(data)}`);
});

/* List buckets */
s3.listBuckets(function(err, data) {
  if (err) console.log(err, err.stack);
  else     console.log(`LN:[41]\n ${JSON.stringify(data)}`);
});

/* putObject: Creates object1 & object2 in felix-test-bucket */
var object1 = {
    Body: '', 
    Bucket: "felix-test-bucket", 
    Key: "HappyFace.jpg"
   };
var object2 = {
    Body: '', 
    Bucket: "felix-test-bucket", 
    Key: "HappyFace.txt", 
    // ServerSideEncryption: "AES256", 
    // StorageClass: "STANDARD_IA"
   };
   s3.putObject(object1, function(err, data) {
    if (err) console.log(err, err.stack);
    else     console.log(`LN:[59]\n ${JSON.stringify(data)}`);
  });
   s3.putObject(object2, function(err, data) {
     if (err) console.log(err, err.stack);
     else     console.log(`LN:[63]\n ${JSON.stringify(data)}`);
     /*
     data = {
      ETag: "\"6805f2cfc46c0f04559748bb039d69ae\"", 
      ServerSideEncryption: "AES256", 
      VersionId: "CG612hodqujkf8FaaNfp8U..FIhLROcp"
     }
     */
   });

/* The following example list two objects in a bucket. */
var listObj = {
    Bucket: "felix-test-bucket", 
    MaxKeys: 2
   };
   s3.listObjects(listObj, function(err, data) {
     if (err) console.log(err, err.stack);
     else     console.log(`LN:[80]\n ${JSON.stringify(data)}`);
     /*
     data = {
      Contents: [
         {
        ETag: "\"70ee1738b6b21e2c8a43f3a5ab0eee71\"", 
        Key: "example1.jpg", 
        LastModified: <Date Representation>, 
        Owner: {
         DisplayName: "myname", 
         ID: "12345example25102679df27bb0ae12b3f85be6f290b936c4393484be31bebcc"
        }, 
        Size: 11, 
        StorageClass: "STANDARD"
       }, 
         {
        ETag: "\"9c8af9a76df052144598c115ef33e511\"", 
        Key: "example2.jpg", 
        LastModified: <Date Representation>, 
        Owner: {
         DisplayName: "myname", 
         ID: "12345example25102679df27bb0ae12b3f85be6f290b936c4393484be31bebcc"
        }, 
        Size: 713193, 
        StorageClass: "STANDARD"
       }
      ], 
      NextMarker: "eyJNYXJrZXIiOiBudWxsLCAiYm90b190cnVuY2F0ZV9hbW91bnQiOiAyfQ=="
     }
     */
   });

/* The following example retrieves an object for an S3 bucket. The request specifies the range header to retrieve a specific byte range. */

var getObj = {
    Bucket: "felix-test-bucket", 
    Key: "HappyFace.jpg",
    Range: "bytes=1-9"
   };
   s3.getObject(getObj, function(err, data) {
     if (err) console.log(err, err.stack);
     else     console.log(`LN:[121]\n${JSON.stringify(data)}`); 
     /*
     data = {
      AcceptRanges: "bytes", 
      ContentLength: 10, 
      ContentRange: "bytes 0-9/43", 
      ContentType: "text/plain", 
      ETag: "\"0d94420ffd0bc68cd3d152506b97a9cc\"", 
      LastModified: <Date Representation>, 
      Metadata: {
      }, 
      VersionId: "null"
     }
     */
   });

   /* The following example retrieves an object metadata. */

 var params = {
    Bucket: "felix-test-bucket", 
    Key: "HappyFace.jpg",
    Range: "bytes=1-9"
   };
   s3.headObject(params, function(err, data) {
     if (err) console.log(err, err.stack);
     else     console.log(`LN:[145]\n ${JSON.stringify(data)}`);
     /*
     data = {
      AcceptRanges: "bytes", 
      ContentLength: 3191, 
      ContentType: "image/jpeg", 
      ETag: "\"6805f2cfc46c0f04559748bb039d69ae\"", 
      LastModified: <Date Representation>, 
      Metadata: {
      }, 
      VersionId: "null"
     }
     */
   });
/*
RESULT LOG GUNCEL:

{
    "name":"S3",
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53268,
    "httpMethod":"GET",
    "httpURL":"/",
    "time":1586852304714,
    "req_id":"7d1e87e91c4b909fc4e2",
    "level":"info",
    "message":"received request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "bytesReceived":0,
    "bodyLength":0,
    "bytesSent":273,
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53268,
    "httpMethod":"GET",
    "httpURL":"/",
    "httpCode":200,
    "time":1586852304724,
    "req_id":"7d1e87e91c4b909fc4e2",
    "elapsed_ms":10.253621,
    "level":"info",
    "message":"responded with XML",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53269,
    "httpMethod":"PUT",
    "httpURL":"/felix-test-bucket",
    "time":1586852304725,
    "req_id":"0ea64e1bbd47a36c21a2",
    "level":"info",
    "message":"received request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "bucketName":"felix-test-bucket",
    "bytesReceived":153,
    "bodyLength":153,
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53269,
    "httpMethod":"PUT",
    "httpURL":"/felix-test-bucket",
    "httpCode":200,
    "time":1586852304734,
    "req_id":"0ea64e1bbd47a36c21a2",
    "elapsed_ms":8.982166,
    "level":"info",
    "message":"responded to request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53270,
    "httpMethod":"PUT",
    "httpURL":"/ilke-test-bucket",
    "time":1586852304735,
    "req_id":"3599e8bc3f498bc6ce50",
    "level":"info",
    "message":"received request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "bucketName":"ilke-test-bucket",
    "bytesReceived":153,
    "bodyLength":153,
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53270,
    "httpMethod":"PUT",
    "httpURL":"/ilke-test-bucket",
    "httpCode":200,
    "time":1586852304736,
    "req_id":"3599e8bc3f498bc6ce50",
    "elapsed_ms":1.289014,
    "level":"info",
    "message":"responded to request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53271,
    "httpMethod":"GET",
    "httpURL":"/",
    "time":1586852304736,
    "req_id":"0c5f5367eb47ad86921b",
    "level":"info",
    "message":"received request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "bytesReceived":0,
    "bodyLength":0,
    "bytesSent":472,
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53271,
    "httpMethod":"GET",
    "httpURL":"/",
    "httpCode":200,
    "time":1586852304738,
    "req_id":"0c5f5367eb47ad86921b",
    "elapsed_ms":1.745921,
    "level":"info",
    "message":"responded with XML",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53272,
    "httpMethod":"PUT",
    "httpURL":"/felix-test-bucket/HappyFace.jpg",
    "time":1586852304738,
    "req_id":"add4a7287d46ae5708bc",
    "level":"info",
    "message":"received request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}

objectPut called!
 !
{
    "name":"S3",
    "bucketName":"felix-test-bucket",
    "objectKey":"HappyFace.jpg",
    "bytesReceived":0,
    "bodyLength":0,
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53272,
    "httpMethod":"PUT",
    "httpURL":"/felix-test-bucket/HappyFace.jpg",
    "contentLength":0,
    "httpCode":200,
    "time":1586852304744,
    "req_id":"add4a7287d46ae5708bc",
    "elapsed_ms":5.408135,
    "level":"info",
    "message":"responded to request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53273,
    "httpMethod":"PUT",
    "httpURL":"/felix-test-bucket/HappyFace.txt",
    "time":1586852304748,
    "req_id":"e8e0d86328459532cb93",
    "level":"info",
    "message":"received request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}

objectPut called!
 !
{
    "name":"S3",
    "bucketName":"felix-test-bucket",
    "objectKey":"HappyFace.txt",
    "bytesReceived":0,
    "bodyLength":0,
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53273,
    "httpMethod":"PUT",
    "httpURL":"/felix-test-bucket/HappyFace.txt",
    "contentLength":0,
    "httpCode":200,
    "time":1586852304749,
    "req_id":"e8e0d86328459532cb93",
    "elapsed_ms":1.090167,
    "level":"info",
    "message":"responded to request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53274,
    "httpMethod":"GET",
    "httpURL":"/felix-test-bucket?max-keys=2",
    "time":1586852304750,
    "req_id":"fe83e59d754da2ac5221",
    "level":"info",
    "message":"received request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}

bucketGet called!
 !
{
    "name":"S3",
    "bucketName":"felix-test-bucket",
    "bytesReceived":0,
    "bodyLength":0,
    "bytesSent":873,
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53274,
    "httpMethod":"GET",
    "httpURL":"/felix-test-bucket?max-keys=2",
    "httpCode":200,
    "time":1586852304753,
    "req_id":"fe83e59d754da2ac5221",
    "elapsed_ms":3.048959,
    "level":"info",
    "message":"responded with XML",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53275,
    "httpMethod":"GET",
    "httpURL":"/felix-test-bucket/HappyFace.txt",
    "time":1586852304753,
    "req_id":"bba557391f4599378c8d",
    "level":"info",
    "message":"received request",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}

objectGet called!
 !
{
    "name":"S3",
    "bucketName":"felix-test-bucket",
    "objectKey":"HappyFace.txt",
    "bytesReceived":0,
    "bodyLength":0,
    "clientIP":"::ffff:127.0.0.1",
    "clientPort":53275,
    "httpMethod":"GET",
    "httpURL":"/felix-test-bucket/HappyFace.txt",
    "contentLength":0,
    "httpCode":200,
    "time":1586852304755,
    "req_id":"bba557391f4599378c8d",
    "elapsed_ms":2.387198,
    "level":"info",
    "message":"responded with only metadata",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
*/
//
//
//
//
//
//
//
//
//
/*
Tam serveri baslatincaki loglar:

yarn run mem_backend (S3BACKEND=mem node index.js)

{
    "name":"S3",
    "time":1586852285649,
    "level":"warn",
    "message":"scality kms unavailable. Using file kms backend unless mem specified.",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10477}
{
    "name":"S3",
    "time":1586852285967,
    "workerId":1,
    "workerPid":10481,
    "level":"info",
    "message":"new worker forked",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10477}
{
    "name":"S3",
    "time":1586852287343,
    "level":"warn",
    "message":"scality kms unavailable. Using file kms backend unless mem specified.",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "time":1586852287520,
    "https":false,
    "level":"info",
    "message":"Http server configuration",
    "hostname":"Ilkes-MacBook-Pro.local",
    "pid":10481}
{
    "name":"S3",
    "time":1586852287524,
    "address":"::",
    "port":8000,
    "pid":10481,
    "level":"info",
    "message":"server started",
    "hostname":"Ilkes-MacBook-Pro.local"}
*/