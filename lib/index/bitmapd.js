require('babel-core/register')({});
require('./bitmapd-utils.js').default.opendb()

const net = require('net');
const HOST = '127.0.0.1';
const PORT = 7000;
const msgarr = [];

var listner1 = function listner1() {
    let msg = msgarr.shift();
    if (!msg)
        return;
    msg = msg.split('#');
    if (msg[0] === '0') {
        require('./utils.js').default.initIndex(msg[1]);
    } else if (msg[0] === '1') {
        const objVal = {
            'content-length': msg[3],
            'content-type': msg[4],
            'last-modified': msg[5],
            acl: JSON.parse(msg[6])
        }
        for (i=7; i<msg.length; i+=2) {
            objVal[msg[i]] = msg[i+1];
        }
        require('./utils.js').default.updateIndex(msg[1], msg[2], objVal);
    } else if (msg[0] === '2') {
        const params = {bucketName: msg[1], prefix: msg[2], marker: msg[3], maxKeys: msg[4], delimiter: msg[5]};
        if (params.prefix === 'undefined')
            params.prefix = undefined;
        if (params.marker === 'undefined')
            params.marker = undefined;
        if (params.delimiter === 'undefined')
            params.delimiter = undefined;
        params.maxKeys = parseInt(params.maxKeys);
        const queryTerms = [];
        for (var i=6; i<msg.length; i++) {
            queryTerms.push(msg[i]);
        }
        require('./utils.js').default.evaluateQuery(queryTerms, params, client);
    } else if (msg[0] === '3') {
        require('./utils.js').default.deleteEntry(msg[1], msg[2]);
    }
}

var client = new net.Socket();
setTimeout(function() {
    client.connect(PORT, HOST, function() {
    client.on('data', function(data) {
        data = data.toString();
        data = data.split('||');
        for (i=0; i<data.length; i++) {
            if (data[i] !== '') {
                msgarr.push(data[i]);
                process.nextTick(listner1);
            }
        }
    });
    client.on('close', function() {
    });
});
}, 1000);
