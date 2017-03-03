import assert from 'assert';
import { sproxydAssert } from '../../../lib/Config';

const dummySproxydConfBoot = {
    sproxyd: {
        bootstrap: ['localhost:8181'],
    },
};

const dummySproxydConfChord = {
    sproxyd: {
        chordCos: '20',
    },
};

const dummySproxydConfBoth = {
    sproxyd: {
        bootstrap: ['localhost:8181'],
        chordCos: '20',
    },
};

describe('sproxydAssert', () => {
    it('should return array containing "bootstrap" if config ' +
        'contains bootstrap', () => {
        const sproxydArray = sproxydAssert(dummySproxydConfBoot.sproxyd);
        assert.strictEqual(sproxydArray.indexOf('bootstrap'), 0);
    });
    it('should return array containing "chordCos" if config contains chordCos',
        () => {
            const sproxydArray = sproxydAssert(dummySproxydConfChord.sproxyd);
            assert.strictEqual(sproxydArray.indexOf('chordCos'), 0);
        });
    it('should return array of "bootstrap" and "chordCos" if config ' +
        'contains both fields', () => {
        const sproxydArray = sproxydAssert(dummySproxydConfBoth.sproxyd);
        assert.strictEqual(sproxydArray.length, 2);
        assert.strictEqual(sproxydArray.indexOf('bootstrap') > -1, true);
        assert.strictEqual(sproxydArray.indexOf('chordCos') > -1, true);
    });
});
