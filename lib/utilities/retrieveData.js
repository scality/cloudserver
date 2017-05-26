function retrieveData(locations, dataRetrievalFn,
    response, logger, errorHandlerFn) {
    if (locations.length === 0) {
        return response.end();
    }
    if (errorHandlerFn === undefined) {
        // eslint-disable-next-line
        errorHandlerFn = () => { response.connection.destroy(); };
    }
    const current = locations.shift();
    return dataRetrievalFn(current, logger,
        (err, readable) => {
            if (err) {
                logger.error('failed to get object', {
                    error: err,
                    method: 'retrieveData',
                });
                return errorHandlerFn(err);
            }
            readable.on('error', err => {
                logger.error('error piping data from source');
                errorHandlerFn(err);
            });
            readable.on('end', () => {
                process.nextTick(retrieveData,
                locations, dataRetrievalFn, response, logger);
            });
            readable.pipe(response, { end: false });
            return undefined;
        });
}


module.exports = retrieveData;
