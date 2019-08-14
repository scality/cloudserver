const data = require('../../../data/wrapper');

function dataDelete(objectGetInfo, log, cb) {
    data.delete(objectGetInfo, log, err => {
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

module.exports = { dataDelete };
