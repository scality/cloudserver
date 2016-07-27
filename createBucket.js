'use strict';

const auth = require('arsenal').auth;
const crypto = require('crypto');
const http = require('http');


function create(hostname,
                port,
                bucketName,
                accessKey,
                secretKey) {
    const options = {
        'method': 'PUT',
        hostname,
        port,
        'path': '/'+bucketName+'/',
        'headers': {
            'Content-Length': 0,
            'host': '127.0.0.1:8000',
            'x-scal-server-side-encryption': 'AES256'
        }
    };
    const request = http.request(options, response => {

        //console.log(response);
        console.log(` <= STATUS: ${response.statusCode}`);
        console.log(` <= HEADERS: ${JSON.stringify(response.headers)}`);
        response.setEncoding('utf8');
        response.on('data', (chunk) => {
            console.log(`BODY: ${chunk}`);
        });
        response.on('end', () => {
            console.log('No more data in response.');
        });
    });

    auth.generateV4Headers(request, '',  accessKey, secretKey, 's3');
    console.log(` => HEADERS: ${JSON.stringify(request._headers)}`);
    //console.log(request);
    request.write('');
    request.end();
}

// module.exports = function main() {
    create('localhost', 8000, 'test14', 'accessKey1', 'verySecretKey1');
// };
