const data = require('../../../data/wrapper');

function dataDelete(locations, method, log, cb) {
    if (!Array.isArray(locations) || locations.length === 0) {
        return process.nextTick(() => cb());
    }
    if (locations.length === 1) {
        return data.delete(locations[0], log, err => {
            if (err) {
                log.error('error deleting object data', {
                    error: err,
                    method: 'dataDelete',
                });
                return cb(err);
            }
            return cb();
        });
    }
    const dataStoreName = locations[0].dataStoreName;
    return data.batchDelete(locations, method, dataStoreName, log, err => {
        if (err) {
            log.error('error batch deleting object data', {
                error: err,
                method: 'dataDelete',
            });
            return cb(err);
        }
        return cb();
    });
}

module.exports = { dataDelete };
