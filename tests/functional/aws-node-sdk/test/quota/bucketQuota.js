const AWS = require('aws-sdk');
const fetch = require('node-fetch');
const { program } = require('commander');

// Configure AWS credentials
AWS.config.update({
  accessKeyId: 'your-access-key-id',
  secretAccessKey: 'your-secret-access-key',
  region: 'your-region'
});

// Function to sign the request
const signRequest = (service, request) => {
  const signer = new AWS.Signers.V4(request, service);
  signer.addAuthorization(AWS.config.credentials, AWS.util.date.getDate());
};

// Function to make the HTTP request
const makeRequest = async (url, options) => {
  const response = await fetch(url, options);
  const data = await response.json();
  return data;
};

// Use commander to handle CLI inputs
program
  .option('-b, --bucket <type>', 'Bucket name')
  .parse(process.argv);

// Call the bucketGetQuota API
const getBucketQuota = async (bucket) => {
  const service = 's3';
  const host = `localhost:8000`;
  const path = '/?bucketGetQuota';
  const url = `http://${host}${path}`;

  const request = {
    method: 'GET',
    host: host,
    path: path,
    headers: {
      'Host': host
    }
  };

  signRequest(service, request);

  const options = {
    method: request.method,
    headers: request.headers
  };

  const data = await makeRequest(url, options);
  console.log(data);
};

getBucketQuota(program.bucket);