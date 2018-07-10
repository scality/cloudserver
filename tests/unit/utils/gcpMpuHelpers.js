const assert = require('assert');
const uuid = require('uuid/v4');
const { createMpuKey, createMpuList } =
    require('../../../lib/data/external/GCP').GcpUtils;

const key = `somekey${Date.now()}`;
const uploadId = uuid().replace(/-/g, '');
const phase = 'createMpulist';
const size = 2;
const correctMpuList = [
    { PartName: `${key}-${uploadId}/${phase}/00001`, PartNumber: 1 },
    { PartName: `${key}-${uploadId}/${phase}/00002`, PartNumber: 2 },
];

describe('GcpUtils MPU Helper Functions:', () => {
    describe('createMpuKey', () => {
        const tests = [
            {
                it: 'if phase and part number are given',
                input: { phase: 'test', partNumber: 1 },
                output: `${key}-${uploadId}/test/00001`,
            },
            {
                it: 'if only phase is given',
                input: { phase: 'test' },
                output: `${key}-${uploadId}/test`,
            },
            {
                it: 'if part number is given',
                input: { partNumber: 1 },
                output: `${key}-${uploadId}/parts/00001`,
            },
            {
                it: 'if phase and part number aren not given',
                input: {},
                output: `${key}-${uploadId}/`,
            },
        ];
        tests.forEach(test => {
            it(test.it, () => {
                const { partNumber, phase } = test.input;
                assert.strictEqual(createMpuKey(
                    key, uploadId, partNumber, phase), test.output);
            });
        });
    });

    describe('createMpuList', () => {
        const tests = [
            {
                it: 'should create valid mpu list',
                input: { phase, size },
                output: correctMpuList,
            },
        ];
        tests.forEach(test => {
            it(test.it, () => {
                const { phase, size } = test.input;
                assert.deepStrictEqual(createMpuList(
                    { Key: key, UploadId: uploadId }, phase, size),
                    test.output);
            });
        });
    });
});
