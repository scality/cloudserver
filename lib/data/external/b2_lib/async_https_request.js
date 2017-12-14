const https = require('https');
const stream = require('stream');

function async_https_request(options, data, returns_stream) {
	let errHandler = {};
	options['User-Agent'] = 'Zenko';
	return new Promise((resolve, reject) => {
		const req = https.request(options, (res) => {
			if (returns_stream) {
				resolve(res);
			}
			else {
				res.setEncoding('utf8');
				res.on('data', (chunk) => {
					if (200 != res.statusCode) {
						err = JSON.parse(chunk);
						errHandler.code = err.status;
						errHandler.description = err.code;
						errHandler.customizeDescription = err.message;
						reject(errHandler);
					}
					else
						resolve(JSON.parse(chunk));
				});
			}
		});
		req.on('error', function(err) {
			if (err.code && err.status && err.message) {
				errHandler.code = err.status;
				errHandler.description = err.code;
				errHandler.customizeDescription = err.message;
			}
			else {
				errHandler.code = 500;
				errHandler.description = 'Internal Error.'
				errHandler.customizeDescription = 'An unexpected error has occurred.';
			}
			reject(errHandler);

		});
		if (undefined !== data && null !== data)
		{
			if ('function' === typeof data.pipe)
				data.pipe(req);
			else
			{
				req.write(data);
				req.end();
			}
		}
		else
			req.end();
	});
}
module.exports = async_https_request;
