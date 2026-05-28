const assert = require('assert');

const bumpMicroVersionId = require('../../../../../lib/api/apiUtils/object/bumpMicroVersionId');

describe('bumpMicroVersionId', () => {
    it('should set a fresh microVersionId when replicationInfo is present', () => {
        const objectMD = { replicationInfo: {} };
        bumpMicroVersionId(objectMD);
        assert(objectMD.microVersionId, 'expected microVersionId to be set');
    });

    it('should produce a different value on each call', () => {
        const objectMD = { replicationInfo: {} };
        bumpMicroVersionId(objectMD);
        const first = objectMD.microVersionId;
        bumpMicroVersionId(objectMD);
        assert.notStrictEqual(objectMD.microVersionId, first);
    });

    it('should do nothing when replicationInfo is absent', () => {
        const objectMD = {};
        bumpMicroVersionId(objectMD);
        assert.strictEqual(objectMD.microVersionId, undefined);
    });

    it('should bump unconditionally when force is true', () => {
        const objectMD = {};
        bumpMicroVersionId(objectMD, true);
        assert(objectMD.microVersionId, 'expected microVersionId to be set when force=true');
    });
});
