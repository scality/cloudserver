const http = require('http');
const aws4 = require('aws4');

// Input AWS access key, secret key, and session token.
const accessKeyId = 'GN20IUQ621RW2YEBBUX1';
const secretAccessKey = 'l09PpDBkZWRp72enn+AxQ8PNF2FquyGRv=/DTA+Z';
const token = '';
const bucketName = 'bucket-1';
// Get the start and end times for a range of one month.
const startTime = new Date(2017, 1, 1, 0, 0, 0, 0).getTime();
const requestBody = JSON.stringify({
    buckets: [bucketName],
    timeRange: [startTime],
});
const header = {
    host: 'localhost',
    port: 8100,
    method: 'POST',
    service: 's3',
    path: '/buckets?Action=ListMetrics',
    signQuery: false,
    body: requestBody,
};
const credentials = { accessKeyId, secretAccessKey, token };
const options = aws4.sign(header, credentials);
const request = http.request(options, response => {
    const body = [];
    response.on('data', chunk => body.push(chunk));
    response.on('end', () => process.stdout.write(`${body.join('')}\n`));
});
request.on('error', e => process.stdout.write(`error: ${e.message}\n`));
request.write(requestBody);
request.end();
