// const AWS = require('aws-sdk');
// const http = require('http'); 
// const crypto = require('crypto');
// const commander = require('commander');

// const AWS = require('aws-sdk');
// const fetch = require('node-fetch');
// const { Command } = require('commander');

// const program = new Command();

// // Configure AWS credentials
// // const credentials = new AWS.EnvironmentCredentials('AWS');
// // console.log('AWS SDK Version:', AWS.VERSION);
// // console.log(process.env.AWS_ACCESS_KEY_ID);
// // console.log(process.env.AWS_SECRET_ACCESS_KEY);
// // // const credentials = new AWS.Credentials(
// // //     process.env.AWS_ACCESS_KEY_ID,
// // //     process.env.AWS_SECRET_ACCESS_KEY
// // // );
// // console.log(credentials);
// // AWS.config.update({
// //     credentials: credentials,
// //     region: process.env.AWS_REGION
// //   });
// // console.log(AWS.config);
// // Function to sign the request

// var Domain = {
//     region: 'us-east-1',
//     endpoint: '127.0.0.1:8000',
// };

// const signRequest = (service, request) => {
//     console.log(`we are here`);
//     const signer = new AWS.Signers.V4(request, service);
//     console.log(signer);
//     console.log(AWS.util.date.getDate());
//     console.log(AWS.config.credentials);
//     signer.addAuthorization(AWS.config.credentials, AWS.util.date.getDate());
//     console.log(signer);
// };

// var endpoint = new AWS.Endpoint(Domain.endpoint);
// /*
//  * The AWS credentials are picked up from the environment.
//  * They belong to the IAM role assigned to the Lambda function.
//  * Since the ES requests are signed using these credentials,
//  * make sure to apply a policy that allows ES domain operations
//  * to the role.
//  */
// var creds = new AWS.EnvironmentCredentials('AWS');

// var req = new AWS.HttpRequest(endpoint);


//     req.method = 'GET';
//     req.path = `/${bucket}/?quota=true`;
//     req.region = Domain.region;
//     req.headers['presigned-expires'] = false;
//     req.headers['Host'] = endpoint.host;
// // Function to make the HTTP request
// // const makeRequest = async (url, options) => {
// //     const response = await fetch(url, options);
// //     console.log(response);
// //     if (response.ok) {
// //         const data = await response.json();
// //         console.log(data);
// //     } else {
// //         console.error(`Request failed with status ${response.status}`);
// //     }
// // };

// var signer = new AWS.Signers.V4(req , 'S3');  // es: service code
// signer.addAuthorization(creds, new Date());


// // Use commander to handle CLI inputs
// program
//     .option('-b, --bucket <type>', 'Bucket name')
//     .parse(process.argv);

// // Call the bucketGetQuota API
// const bucketGetQuota = async (bucket) => {
//     const service = 's3';
//     const host = '127.0.0.1:8000';
//     const path = `/${bucket}/?quota=true`;
//     const url = `http://${host}${path}`;
//     const endpoint = new AWS.Endpoint(host);

//     const request = {
//         method: 'GET',
//         host: host,
//         path: path,
//         body:'',
//         headers: {
//             'Host': host,
//             'X-Amz-Date': new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '')
//         },
//         endpoint: endpoint
//     };
//     console.log(request);
//     signRequest('s3', request);
//     console.log(`we are here`);

//     const options = {
//         method: request.method,
//         headers: request.headers
//     };

//     await makeRequest(url, options);
// };

// bucketGetQuota(program.bucket);


// const AWS = require('aws-sdk');
// const http = require('http');
// const commander = require('commander');

// const signRequest = (service, request) => {
//     const creds = new AWS.Credentials({
//         accessKeyId: process.env.AWS_ACCESS_KEY_ID,
//         secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
//     });
//     const signer = new AWS.Signers.V4(request, service);
//     signer.addAuthorization(creds, new Date());
//     console.log('Request signed');
// };

// const bucketGetQuota = async (bucket) => {
//     const service = 's3';
//     const host = '127.0.0.1:8000';
//     const path = `/${bucket}/?quota=true`;
//     const endpoint = new AWS.Endpoint(host);
//     console.log(endpoint);

//     const request = new AWS.HttpRequest(endpoint);
//     request.method = 'GET';
//     request.path = path;
//     request.body = '';
//     request.headers['Host'] = host;
//     request.headers['X-Amz-Date'] = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
//     request.region = 'us-east-1';

//     signRequest(service, request);

//     const options = {
//         method: request.method,
//         headers: request.headers,
//         port: endpoint.port
//     };

//     const req = http.request(options, (res) => {
//         let data = '';
//         res.on('data', (chunk) => {
//             data += chunk;
//         });
//         res.on('end', () => {
//             console.log(data);
//         });
//     });

//     req.on('error', (e) => {
//         console.error(e);
//     });

//     req.end();
// };

// commander
//     .version('0.0.1')
//     .arguments('<bucket>')
//     .action((bucket) => {
//         bucketGetQuota(bucket);
//     })
//     .parse(process.argv);


// const AWS = require('aws-sdk');
// const commander = require('commander');

// // Configure AWS SDK
// AWS.config.update({
//     region: process.env.AWS_REGION,
//     accessKeyId: process.env.AWS_ACCESS_KEY_ID,
//     secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
// });

// console.log(AWS.config);

// const signRequest = (service, request) => {
//     const signer = new AWS.Signers.V4(request, service);
//     signer.addAuthorization(AWS.config.credentials, new Date());
//     console.log('Request signed');
// };

// const bucketGetQuota = async (bucket) => {
//     const service = 's3';
//     const host = '127.0.0.1:8000';
//     const path = `/${bucket}/?quota=true`;
//     const endpoint = new AWS.Endpoint(host);
//     console.log(endpoint);

//     const request = new AWS.HttpRequest(endpoint);
//     request.method = 'GET';
//     request.path = path;
//     request.body = '';
//     request.headers['Host'] = host;
//     request.headers['X-Amz-Date'] = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
//     request.region = 'us-east-1';
//     signRequest(service, request);

//     const awsRequest = new AWS.Request(request);
//     awsRequest.send((err, data) => {
//         if (err) {
//             console.error(err);
//         } else {
//             console.log(data);
//         }
//     });
// };

// commander
//     .version('0.0.1')
//     .arguments('<bucket>')
//     .action((bucket) => {
//         bucketGetQuota(bucket);
//     })
//     .parse(process.argv);

// const AWS = require('aws-sdk');
// const http = require('http'); 
// const crypto = require('crypto');
// const commander = require('commander');

// // Configure AWS SDK
// AWS.config.update({
//     region: 'us-east-1', 
//     accessKeyId: 'FU2JJQLZDD1E4N0G2XBI', 
//     secretAccessKey: 'U=3xXqLpN0U9UDxtjHmtiLY9qSQH2aK=BpG=H8j6'
// });

// const bucketGetQuota = async (bucket) => {
//     const service = 's3';
//     const host = '127.0.0.1:8000';
//     const path = `/${bucket}/?quota=true`;
//     const endpoint = new AWS.Endpoint(host);

//     const request = new AWS.HttpRequest(endpoint);
//     request.method = 'GET';
//     request.path = path;
//     request.body = '';
//     request.headers['Host'] = host;
//     request.headers['X-Amz-Date'] = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
//     const sha256hash = crypto.createHash('sha256').update(request.body).digest('hex');
//     request.headers['X-Amz-Content-SHA256'] = sha256hash;
//     request.region = 'us-east-1';

//     // Sign the request
//     const signer = new AWS.Signers.V4(request, service);
//     signer.addAuthorization(AWS.config.credentials, new Date());
//     console.log(`signed request ${JSON.stringify(request)}`);

//     const options = {
//         hostname: host.split(':')[0],
//         port: host.split(':')[1],
//         path: request.path,
//         method: request.method,
//         headers: request.headers
//     };
//     console.log(options);

//     const req = http.request(options, (res) => {
//         let data = '';
//         res.on('data', (chunk) => {
//             data += chunk;
//         });
//         res.on('end', () => {
//             console.log(data);
//         });
//     });

//     req.on('error', (e) => {
//         console.error(e);
//     });

//     req.end();
// };

// commander
//     .version('0.0.1')
//     .arguments('<bucket>')
//     .action((bucket) => {
//         bucketGetQuota(bucket);
//     })
//     .parse(process.argv);

// const AWS = require('aws-sdk');
// const http = require('http');
// const commander = require('commander');

// const sendRequest = async (method, host, path, headers = {}, body = '') => {
//     const service = 's3';
//     const endpoint = new AWS.Endpoint(host);

//     const request = new AWS.HttpRequest(endpoint);
//     request.method = method.toUpperCase();
//     request.path = path;
//     request.body = body;
//     request.headers = headers;
//     request.headers['Host'] = host;
//     request.headers['X-Amz-Date'] = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
//     const sha256hash = AWS.util.crypto.sha256(request.body || '', 'hex');
//     request.headers['X-Amz-Content-SHA256'] = sha256hash;
//     request.region = AWS.config.region;

//     // Sign the request
//     const signer = new AWS.Signers.V4(request, service);
//     signer.addAuthorization(AWS.config.credentials, new Date());

//     const options = {
//         hostname: host.split(':')[0],
//         port: host.split(':')[1],
//         path: request.path,
//         method: request.method,
//         headers: request.headers
//     };

//     const req = http.request(options, (res) => {
//         let data = '';
//         res.on('data', (chunk) => {
//             data += chunk;
//         });
//         res.on('end', () => {
//             console.log(data);
//         });
//     });

//     req.on('error', (e) => {
//         console.error(e);
//     });

//     req.write(body);
//     req.end();
// };

// commander
//     .version('0.0.1')
//     .arguments('<method> <host> <path> [headers] [body]')
//     .action((method, host, path, headers, body) => {
//         sendRequest(method, host, path, JSON.parse(headers || '{}'), body);
//     })
//     .parse(process.argv);


const fetch = require('node-fetch');
const AWS = require('aws-sdk');
const commander = require('commander');

const sendRequest = async (method, host, path, body = '') => {
    const service = 's3';
    const endpoint = new AWS.Endpoint(host);

    const request = new AWS.HttpRequest(endpoint);
    request.method = method.toUpperCase();
    request.path = path;
    request.body = body;
    request.headers['Host'] = host;
    request.headers['X-Amz-Date'] = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
    const sha256hash = AWS.util.crypto.sha256(request.body || '', 'hex');
    request.headers['X-Amz-Content-SHA256'] = sha256hash;
    request.region = AWS.config.region;

    const signer = new AWS.Signers.V4(request, service);
    signer.addAuthorization(AWS.config.credentials, new Date());

    const url = `http://${host}${path}`;
    const options = {
        method: request.method,
        headers: request.headers
    };

    if (method !== 'GET' && method !== 'HEAD') {
        options.body = request.body;
    }

    try {
        const response = await fetch(url, options);
        const data = await response.text();
        console.log(data);
    } catch (error) {
        console.error(error);
    }
};

commander
    .version('0.0.1')
    .arguments('<method> <host> <path> [body]')
    .action((method, host, path, body) => {
        sendRequest(method, host, path, body);
    })
    .parse(process.argv);


