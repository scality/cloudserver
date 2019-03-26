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
    test('should throw an error if bootstrap list is not an array', () => {
        expect(() => {
            sproxydAssert(makeSproxydConf('localhost:8181'));
        }).toThrow();
    });
    test(
        'should throw an error if bootstrap array does not contain strings',
        () => {
            expect(() => {
                sproxydAssert(makeSproxydConf([8181]));
            }).toThrow();
        }
    );
    test('should throw an error if chordCos is not a string', () => {
        expect(() => {
            sproxydAssert(makeSproxydConf(null, 20));
        }).toThrow();
    });
    test('should throw an error if chordCos is more than 1 digit', () => {
        expect(() => {
            sproxydAssert(makeSproxydConf(null, '200'));
        }).toThrow();
    });
    test('should throw an error if chordCos bigger than 7', () => {
        expect(() => {
            sproxydAssert(makeSproxydConf(null, '7'));
        }).toThrow();
    });
    test('should throw an error if path is not a string', () => {
        expect(() => {
            sproxydAssert(makeSproxydConf(null, null, 20));
        }).toThrow();
    });

    test('should return array containing "bootstrap" if config ' +
        'contains bootstrap', () => {
        const sproxydArray = sproxydAssert(makeSproxydConf(['localhost:8181']));
        expect(sproxydArray.indexOf('bootstrap')).toBe(0);
    });
    test(
        'should return array containing "chordCos" if config contains chordCos',
        () => {
            const sproxydArray = sproxydAssert(makeSproxydConf(null, '2'));
            expect(sproxydArray.indexOf('chordCos')).toBe(0);
        }
    );
    test('should return array containing "path" if config contains path', () => {
        const sproxydArray = sproxydAssert(
            makeSproxydConf(null, null, '/proxy/arc'));
        expect(sproxydArray.indexOf('path')).toBe(0);
    });
    test('should return array of "bootstrap", "chordCos", and "path" if config ' +
        'contains all fields', () => {
        const sproxydArray = sproxydAssert(
            makeSproxydConf(['localhost:8181'], '2', '/proxy/arc'));
        expect(sproxydArray.length).toBe(3);
        expect(sproxydArray.indexOf('bootstrap') > -1).toBe(true);
        expect(sproxydArray.indexOf('chordCos') > -1).toBe(true);
        expect(sproxydArray.indexOf('path') > -1).toBe(true);
    });
});
