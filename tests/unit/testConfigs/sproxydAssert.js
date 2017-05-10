const assert = require('assert');
const { sproxydAssert } = require('../../../lib/Config');

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

describe('sproxydAssert', () => {
    it('should throw an error if bootstrap list is not an array', () => {
        assert.throws(() => {
            sproxydAssert(makeSproxydConf('localhost:8181'));
        },
        /bad config: sproxyd.bootstrap must be an array of strings/);
    });
    it('should throw an error if bootstrap array does not contain strings',
    () => {
        assert.throws(() => {
            sproxydAssert(makeSproxydConf([8181]));
        },
        /bad config: sproxyd.bootstrap must be an array of strings/);
    });
    it('should throw an error if chordCos is not a string', () => {
        assert.throws(() => {
            sproxydAssert(makeSproxydConf(null, 20));
        },
        /bad config: sproxyd.chordCos must be a string/);
    });
    it('should throw an error if chordCos is more than 2 digits', () => {
        assert.throws(() => {
            sproxydAssert(makeSproxydConf(null, '200'));
        },
        /bad config: sproxyd.chordCos must be a 2hex-chars string/);
    });
    it('should throw an error if chordCos includes non-hex digit', () => {
        assert.throws(() => {
            sproxydAssert(makeSproxydConf(null, '2z'));
        },
        /bad config: sproxyd.chordCos must be a 2hex-chars string/);
    });
    it('should throw an error if path is not a string', () => {
        assert.throws(() => {
            sproxydAssert(makeSproxydConf(null, null, 20));
        }, /bad config: sproxyd.path must be a string/);
    });

    it('should return array containing "bootstrap" if config ' +
        'contains bootstrap', () => {
        const sproxydArray = sproxydAssert(makeSproxydConf(['localhost:8181']));
        assert.strictEqual(sproxydArray.indexOf('bootstrap'), 0);
    });
    it('should return array containing "chordCos" if config contains chordCos',
        () => {
            const sproxydArray = sproxydAssert(makeSproxydConf(null, '20'));
            assert.strictEqual(sproxydArray.indexOf('chordCos'), 0);
        });
    it('should return array containing "path" if config contains path', () => {
        const sproxydArray = sproxydAssert(
            makeSproxydConf(null, null, '/proxy/arc'));
        assert.strictEqual(sproxydArray.indexOf('path'), 0);
    });
    it('should return array of "bootstrap", "chordCos", and "path" if config ' +
        'contains all fields', () => {
        const sproxydArray = sproxydAssert(
            makeSproxydConf(['localhost:8181'], '20', '/proxy/arc'));
        assert.strictEqual(sproxydArray.length, 3);
        assert.strictEqual(sproxydArray.indexOf('bootstrap') > -1, true);
        assert.strictEqual(sproxydArray.indexOf('chordCos') > -1, true);
        assert.strictEqual(sproxydArray.indexOf('path') > -1, true);
    });
});
