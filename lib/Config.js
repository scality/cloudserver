import assert from 'assert';
import fs from 'fs';
import path from 'path';

/**
 * Reads from a config file and returns the content as a config object
 */
export default class Config {
    constructor() {
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the S3_CONFIG_FILE environment var.
         */
        this.path = path.join(__dirname, '../config.json');
        if (process.env.S3_CONFIG_FILE !== undefined) {
            this.path = process.env.S3_CONFIG_FILE;
        }

        // Read config automatically
        this._getConfig();
    }

    _getConfig() {
        let config;
        try {
            const data = fs.readFileSync(this.path, { encoding: 'utf-8' });
            config = JSON.parse(data);
        } catch (err) {
            throw new Error(`could not parse config file: ${err.message}`);
        }

        this.port = 8000;
        if (config.port !== undefined) {
            assert(Number.isInteger(config.port) && config.port > 0,
                   'bad config: port must be a positive integer');
            this.port = config.port;
        }

        assert(typeof config.regions === 'object',
               'bad config: the list of regions is mandatory');
        assert(Object.keys(config.regions).every(
               r => typeof r === 'string' && config.regions[r] instanceof Array
               && config.regions[r].every(e => typeof e === 'string')),
               'bad config: regions must be a set of {region: [endpoints]}');
        this.regions = config.regions;

        this.clusters = false;
        if (config.clusters !== undefined) {
            assert(Number.isInteger(config.clusters) && config.clusters > 0,
                   'bad config: clusters must be a positive integer');
            this.clusters = config.clusters;
        }

        this.sproxyd = { bootstrap: [] };
        if (config.sproxyd !== undefined) {
            if (config.sproxyd.bootstrap !== undefined) {
                assert(Array.isArray(config.sproxyd.bootstrap)
                    && config.sproxyd.bootstrap
                             .every(e => typeof e === 'string'),
                    'bad config: sproxyd.bootstrap must be a list of strings');
                assert(config.sproxyd.bootstrap.length > 0,
                       'sproxyd bootstrap list is empty');
                this.sproxyd.bootstrap = config.sproxyd.bootstrap;
            }
            if (config.sproxyd.chordCos !== undefined) {
                assert(typeof config.sproxyd.chordCos === 'string',
                       'bad config: sproxyd.chordCos must be a string');
                assert(config.sproxyd.chordCos.match(/^[0-9a-fA-F]{2}$/),
                    'bad config: sproxyd.chordCos must be a 2hex-chars string');
                this.sproxyd.chordCos =
                    Number.parseInt(config.sproxyd.chordCos, 16);
            }
        }

        this.bucketd = { bootstrap: [] };
        if (config.bucketd !== undefined
                && config.bucketd.bootstrap !== undefined) {
            assert(config.bucketd.bootstrap instanceof Array
                   && config.bucketd.bootstrap.every(
                       e => typeof e === 'string'),
                   'bad config: bucketd.bootstrap must be a list of strings');
            this.bucketd.bootstrap = config.bucketd.bootstrap;
        }

        this.vaultd = {};
        if (config.vaultd) {
            if (config.vaultd.port !== undefined) {
                assert(Number.isInteger(config.vaultd.port)
                       && config.vaultd.port > 0,
                       'bad config: vaultd port must be a positive integer');
                this.vaultd.port = config.vaultd.port;
            }
            if (config.vaultd.host !== undefined) {
                assert.strictEqual(typeof config.vaultd.host, 'string',
                                   'bad config: vaultd host must be a string');
                this.vaultd.host = config.vaultd.host;
            }
        }

        this.log = { logLevel: 'debug', dumpLevel: 'error' };
        if (config.log !== undefined) {
            if (config.log.logLevel !== undefined) {
                assert(typeof config.log.logLevel === 'string',
                       'bad config: log.logLevel must be a string');
                this.log.logLevel = config.log.logLevel;
            }
            if (config.log.dumpLevel !== undefined) {
                assert(typeof config.log.dumpLevel === 'string',
                        'bad config: log.dumpLevel must be a string');
                this.log.dumpLevel = config.log.dumpLevel;
            }
        }
    }
}
