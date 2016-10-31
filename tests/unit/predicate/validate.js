import assert from 'assert';
import validate from '../../../lib/predicate/validate';
import SwError from 'swerrs';

const s3spec = {
    bucket: {
        name: /^foo$/,
        ownerIdentity: {
            principalId(v) {
                return (v && v.length === 14);
            },
        },
    },
    object: {
        key: /Foobar\/Uploads\/(p\d+)\/(\1)\S+\.\w+$/,
    },
};
const record = {
    s3: {
        s3SchemaVersion: '1.0',
        configurationId: '52547aab-3108-46c9-8f7e-aeeb1e8dbe7d',
        bucket: {
            name: 'foo',
            ownerIdentity: {
                principalId: 'A292DSXXXXXXXL',
            },
            arn: 'arn:aws:s3:::foo',
        },
        object: {
            key: 'Foobar/Uploads/p65579533/p65579533_r2829_sYMLWFTWU_q119.jpg',
            size: 4835360,
            eTag: 'b0e53adbb2e0dca75ea2adaed40f65de',
            versionId: 'WqLL64QL1gSLDlpvMbVkuViQHbDdgm4n',
            sequencer: '0057E05FB0344DFEF8',
        },
    },
};

describe('predicate.validate', () => {
    it('should validate lambda record s3 object', () => {
        const errs = validate(s3spec, record.s3);
        assert.equal(0, errs.length);
    });

    it('should return errors when called with wrong object', () => {
        const errs = validate(s3spec, record);
        assert.equal(2, errs.length);
    });

    it('should return proper messages for errors found in arrays', () => {
        const errs = validate({
            foo: [/^(?:duck|cow|chicken)$/],
        }, {
            foo: ['duck', 'cow', 'rooster'],
        });
        assert.equal(1, errs.length);
        assert.equal('$.foo.2: invalid string pattern', errs[0]);
    });

    it('should return proper messages for nested objects', () => {
        const errs = validate({
            bar: {
                foo: [/^(?:duck|cow|chicken)$/],
                bar: [{
                    a: 1,
                    b: true,
                    c: 'three',
                }],
                baz: {
                    a: 1,
                    b: true,
                    c: 'three',
                },
            },
        }, {
            bar: {
                foo: ['duck', 'cow', 'rooster'],
                bar: [{
                    a: 1,
                    b: true,
                    c: 'three',
                }, {
                    a: 'one',
                    b: 2,
                    c: 3,
                }],
                baz: {
                    a: 42,
                    b: 'false',
                    c: 3,
                },
            },
        });
        assert.equal(6, errs.length);
        assert.deepEqual([
            '$.bar.foo.2: invalid string pattern',
            '$.bar.bar.1.a: want number, have string',
            '$.bar.bar.1.b: want boolean, have number',
            '$.bar.bar.1.c: want string, have number',
            '$.bar.baz.b: want boolean, have string',
            '$.bar.baz.c: want string, have number',
        ], errs);
    });

    it('should correctly use context object ' +
      'when calling validation functions', () => {
        const ctx = {};
        const errs = validate({
            foo: [/^(?:duck|cow|chicken)$/],
            bar: {
                baz(v) {
                    if (typeof v === 'number') {
                        this.baz = v + 1;
                        return true;
                    }
                    return false;
                },
            },
        }, {
            foo: ['cow', 'duck'],
            bar: {
                baz: 41,
            },
        }, ctx);
        assert.equal(0, errs.length);
        assert.deepEqual({
            baz: 42,
        }, ctx);
    });

    it('should correctly validate primitive objects', () => {
        const data = [{
            spec: 1,
            candidate: 0,
            n: 0,
        }, {
            spec: '',
            candidate: 'cat',
            n: 0,
        }, {
            spec: /^cat$/,
            candidate: 'dog',
            n: 1,
        }];

        data.forEach(d => {
            const errs = validate(d.spec, d.candidate);
            assert.equal(d.n, errs.length);
        });
    });

    it('should correctly populate provided errors object', () => {
        const data = [{
            spec: 1,
            candidate: 0,
            n: 0,
        }, {
            spec: '',
            candidate: 'cat',
            n: 0,
        }, {
            spec: /^cat$/,
            candidate: 'dog',
            n: 1,
        }];

        data.forEach(d => {
            const errs = [];
            const verrs = validate(d.spec, d.candidate, errs);
            assert.equal(d.n, errs.length);
            assert.strictEqual(errs, verrs);

            const swerr = new SwError();
            const swerrs = validate(d.spec, d.candidate, swerr);

            assert.equal(d.n, swerr.values.length);
            assert.strictEqual(swerr, swerrs);
        });
    });
});
