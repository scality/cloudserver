import assert from 'assert';
import { sproxydAssert } from '../../../lib/Config';

const dummySproxydConfBootstrap = {
    sproxyd: {
        bootstrap: ['localhost:8181'],
    },
};

const dummySproxydConfChordcos = {
    sproxyd: {
        chordCos: '20',
    },
};

describe('sproxydAssert', () => {
    it('should return "bootstrap" if config contains bootstrap', () => {
        const resString = sproxydAssert(dummySproxydConfBootstrap.sproxyd);
        assert.strictEqual(resString, 'bootstrap');
    });
    it('should return "chordCos" if config contains chordCos', () => {
        const resString = sproxydAssert(dummySproxydConfChordcos.sproxyd);
        assert.strictEqual(resString, 'chordCos');
    });
});
