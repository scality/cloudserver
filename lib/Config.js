import assert from 'assert';
import fs from 'fs';
import path from 'path';

import authDataChecker from './auth/in_memory/checker';

/**
 * Reads from a config file and returns the content as a config object
 */
class Config {
    constructor() {
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the S3_CONFIG_FILE environment var.
         */
        this._basePath = path.join(__dirname, '..');
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

        this.utapi = {};
        if (config.utapi) {
            if (config.utapi.port) {
                assert(Number.isInteger(config.utapi.port)
                    && config.utapi.port > 0,
                    'bad config: utapi port must be a positive integer');
                this.utapi.port = config.utapi.port;
            }
            if (config.utapi.workers !== undefined) {
                assert(Number.isInteger(config.utapi.workers)
                    && config.utapi.workers > 0,
                    'bad config: utapi workers must be a positive integer');
                this.utapi.workers = config.utapi.workers;
            }
            if (config.utapi.redis) {
                if (config.utapi.redis.sentinels) {
                    this.utapi.redis = { sentinels: [], name: null };

                    assert(typeof config.utapi.redis.name === 'string',
                        'bad config: redis sentinel name must be a string');
                    this.utapi.redis.name = config.utapi.redis.name;

                    assert(Array.isArray(config.utapi.redis.sentinels),
                        'bad config: redis sentinels must be an array');
                    config.utapi.redis.sentinels.forEach(item => {
                        const { host, port } = item;
                        assert(typeof host === 'string',
                            'bad config: redis sentinel host must be a string');
                        assert(typeof port === 'number',
                            'bad config: redis sentinel port must be a number');
                        this.utapi.redis.sentinels.push({ host, port });
                    });
                } else {
                    // check for standalone configuration
                    this.utapi.redis = {};
                    assert(typeof config.utapi.redis.host === 'string',
                        'bad config: redis.host must be a string');
                    assert(typeof config.utapi.redis.port === 'number',
                        'bad config: redis.port must be a number');
                    this.utapi.redis.host = config.utapi.redis.host;
                    this.utapi.redis.port = config.utapi.redis.port;
                }
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

        if (config.certFilePaths) {
            assert(typeof config.certFilePaths === 'object' &&
                typeof config.certFilePaths.key === 'string' &&
                typeof config.certFilePaths.cert === 'string' && ((
                    config.certFilePaths.ca &&
                    typeof config.certFilePaths.ca === 'string') ||
                    !config.certFilePaths.ca)
               );
        }
        const { key, cert, ca } = config.certFilePaths ?
            config.certFilePaths : {};
        if (key && cert) {
            const keypath = (key[0] === '/') ? key : `${this._basePath}/${key}`;
            const certpath = (cert[0] === '/') ?
                cert : `${this._basePath}/${cert}`;
            let capath = undefined;
            if (ca) {
                capath = (ca[0] === '/') ? ca : `${this._basePath}/${ca}`;
                assert.doesNotThrow(() =>
                    fs.accessSync(capath, fs.F_OK | fs.R_OK),
                    `File not found or unreachable: ${capath}`);
            }
            assert.doesNotThrow(() =>
                fs.accessSync(keypath, fs.F_OK | fs.R_OK),
                `File not found or unreachable: ${keypath}`);
            assert.doesNotThrow(() =>
                fs.accessSync(certpath, fs.F_OK | fs.R_OK),
                `File not found or unreachable: ${certpath}`);
            this.https = {
                cert: fs.readFileSync(certpath, 'ascii'),
                key: fs.readFileSync(keypath, 'ascii'),
                ca: ca ? fs.readFileSync(capath, 'ascii') : undefined,
            };
            this.httpsPath = {
                ca: capath,
                cert: certpath,
            };
        } else if (key || cert) {
            throw new Error('bad config: both certFilePaths.key and ' +
                'certFilePaths.cert must be defined');
        }
        /**
         * Configure the backends for Authentication, Data and Metadata.
         */
        let auth = 'mem';
        let data = 'file';
        let metadata = 'file';
        let kms = 'file';
        const dp = (process.env.ENABLE_DP === 'true');
        if (process.env.S3BACKEND) {
            const validBackends = ['mem', 'file', 'scality'];
            assert(validBackends.indexOf(process.env.S3BACKEND) > -1,
                'bad environment variable: S3BACKEND environment variable ' +
                'should be one of mem/file/scality'
            );
            auth = process.env.S3BACKEND;
            data = process.env.S3BACKEND;
            metadata = process.env.S3BACKEND;
            kms = process.env.S3BACKEND;
        }
        if (process.env.S3VAULT) {
            auth = process.env.S3VAULT;
        }
        if (auth === 'file' || auth === 'mem') {
            // Auth only checks for 'mem' since mem === file
            auth = 'mem';
            let authfile = `${__dirname}/../conf/authdata.json`;
            if (process.env.S3AUTH_CONFIG) {
                authfile = process.env.S3AUTH_CONFIG;
            }
            const authData = require(authfile);
            if (authDataChecker(authData)) {
                throw new Error('bad config: invalid auth config file.');
            }
            this.authData = authData;
        }
        if (process.env.S3SPROXYD) {
            data = process.env.S3SPROXYD;
        }
        if (process.env.S3METADATA) {
            metadata = process.env.S3METADATA;
        }
        if (process.env.S3KMS) {
            kms = process.env.S3KMS;
        }
        this.backends = {
            auth,
            data,
            metadata,
            kms,
            dp,
        };

        /**
         * Configure the file paths for data and metadata
         * if using the file backend.  If no path provided,
         * uses data and metadata at the root of the S3 project directory
         */
        const dataPath = process.env.S3DATAPATH ?
            process.env.S3DATAPATH : `${__dirname}/../localData`;
        const metadataPath = process.env.S3METADATAPATH ?
            process.env.S3METADATAPATH : `${__dirname}/../localMetadata`;
        this.filePaths = {
            dataPath,
            metadataPath,
        };
        return config;
    }
}

export default new Config();
