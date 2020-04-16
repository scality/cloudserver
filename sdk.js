
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
/* The following example creates a bucket. */
 var params1 = {
  Bucket: "felix-test-bucket"
 };
 var params2 = {
  Bucket: "ilke-test-bucket"
 };
s3.createBucket(params1, function(err, data) {
   if (err) console.log(err, err.stack); // an error occurred
   else     console.log(data);           // successful response
});
s3.createBucket(params2, function(err, data) {
   if (err) console.log(err, err.stack); // an error occurred
   else     console.log(data);           // successful response
});
s3.listBuckets(function(err, data) {
  if (err) console.log(err, err.stack); // an error occurred
  else     console.log(data);           // successful response
});
