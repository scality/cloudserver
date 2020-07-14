const assert = require('assert');
const { parseSproxydConfig } = require('../../../lib/Config');

function makeSproxydConf(bootstrap, chordCos, sproxydPath) {
    const conf = {
        sproxyd: {},
    };
    if (bootstrap) {
        conf.sproxyd.bootstrap = bootstrap;
    }
    if (chordCos) {
        conf.sproxyd.chordCos = chordCos;
    }
    if (sproxydPath) {
        conf.sproxyd.path = sproxydPath;
    }
    return conf.sproxyd;
}

describe('parseSproxydConfig', () => {
    it('should return a parsed config if valid', () => {
        const sproxydConf = parseSproxydConfig(makeSproxydConf(
            ['localhost:8181'], null, '/arc'));
        assert.deepStrictEqual(sproxydConf, {
            bootstrap: ['localhost:8181'],
            path: '/arc',
        });
    });
    it('should return a parsed config with chordCos if valid', () => {
        const sproxydConf = parseSproxydConfig(makeSproxydConf(
            ['localhost:8181'], '3', '/arc'));
        assert.deepStrictEqual(sproxydConf, {
            bootstrap: ['localhost:8181'],
            path: '/arc',
            chordCos: 3,
        });
    });
    it('should throw an error if bootstrap list is not an array', () => {
        assert.throws(() => {
            parseSproxydConfig(makeSproxydConf('localhost:8181'));
        });
    });
    it('should throw an error if bootstrap array does not contain strings',
    () => {
        assert.throws(() => {
            parseSproxydConfig(makeSproxydConf([8181]));
        });
    });
    it('should throw an error if chordCos is more than 1 digit', () => {
        assert.throws(() => {
            parseSproxydConfig(makeSproxydConf(null, '200'));
        });
    });
    it('should throw an error if chordCos is a floating point value', () => {
        assert.throws(() => {
            parseSproxydConfig(makeSproxydConf(null, '3.5'));
        });
    });
    it('should throw an error if chordCos bigger than 6', () => {
        assert.throws(() => {
            parseSproxydConfig(makeSproxydConf(null, '7'));
        });
    });
    it('should throw an error if path is not a string', () => {
        assert.throws(() => {
            parseSproxydConfig(makeSproxydConf(null, null, 20));
        });
    });
});
