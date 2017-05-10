const Redis = require('ioredis');

const logger = require('./utilities/logger');

module.exports = class RedisClient {
    /**
    * @constructor
    * @param {string} host - Redis host
    * @param {number} port - Redis port
    */
    constructor(host, port) {
        this._client = new Redis({
            host,
            port,
        });
        this._client.on('error', err =>
            logger.trace('error from redis', {
                error: err,
                method: 'RedisClient.constructor',
                redisHost: host,
                redisPort: port,
            })
        );
        return this;
    }

    /**
    * increment value of a key by 1 and set a ttl
    * @param {string} key - key holding the value
    * @param {number} expiry - expiry in seconds
    * @param {callback} cb - callback
    * @return {undefined}
    */
    incrEx(key, expiry, cb) {
        return this._client
            .multi([['incr', key], ['expire', key, expiry]])
            .exec(cb);
    }


    /**
    * execute a batch of commands
    * @param {string[]} cmds - list of commands
    * @param {callback} cb - callback
    * @return {undefined}
    */
    batch(cmds, cb) {
        return this._client.pipeline(cmds).exec(cb);
    }

    clear(cb) {
        return this._client.flushDb(cb);
    }
};
