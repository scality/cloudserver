const assert = require('assert');

const setPartRanges = require('../../../lib/api/apiUtils/object/setPartRanges');

describe('setPartRanges function', () => {
    it('should set range on a one part object', () => {
        const dataLocations = [{ key: '1' }];
        const outerRange = [2, 8];
        const actual =
            setPartRanges(dataLocations, outerRange);
        assert.deepStrictEqual(actual, [{
            key: '1',
            range: [2, 8],
        }]);
    });

    it('for a 3-part object, should include full first part, set range on ' +
        'middle part and exclude last part if range request starts at 0' +
        'and ends in the middle of the second part',
        () => {
            const dataLocations = [{ key: '1', size: '4', start: '0' },
                { key: '2', size: '10', start: '4' },
                { key: '3', size: '20', start: '14' },
            ];
            const outerRange = [0, 10];
            const actual =
                setPartRanges(dataLocations, outerRange);
            assert.deepStrictEqual(actual, [{ key: '1', size: '4', start: '0' },
                { key: '2', size: '7', start: '4', range: [0, 6] }]);
        });

    it('for a 3-part object, should include part of first part, all of ' +
        'second part and part of third part if range request starts within ' +
        'first part and ends before end of last part',
        () => {
            const dataLocations = [{ key: '1', size: '4', start: '0' },
                { key: '2', size: '10', start: '4' },
                { key: '3', size: '20', start: '14' },
            ];
            const outerRange = [2, 18];
            const actual =
                setPartRanges(dataLocations, outerRange);
            assert.deepStrictEqual(actual, [{ key: '1', size: '2', start: '0',
                range: [2, 3] },
            { key: '2', size: '10', start: '4' },
            { key: '3', size: '5', start: '14', range: [0, 4] },
            ]);
        });

    it('for a 3-part object, should include only a range of the middle part ' +
        'if the range excludes both the beginning and the end',
        () => {
            const dataLocations = [{ key: '1', size: '4', start: '0' },
                { key: '2', size: '10', start: '4' },
                { key: '3', size: '20', start: '14' },
            ];
            const outerRange = [5, 7];
            const actual =
                setPartRanges(dataLocations, outerRange);
            assert.deepStrictEqual(actual, [{ key: '2', size: '3', start: '4',
                range: [1, 3] },
            ]);
        });

    it('for a 3-part object, should include only a range of the middle part ' +
        'and all of the third part if the range excludes a portion of the ' +
        'beginning',
        () => {
            const dataLocations = [{ key: '1', size: '4', start: '0' },
                { key: '2', size: '10', start: '4' },
                { key: '3', size: '20', start: '14' },
            ];
            const outerRange = [5, 34];
            const actual =
                setPartRanges(dataLocations, outerRange);
            assert.deepStrictEqual(actual, [{ key: '2', size: '9', start: '4',
                range: [1, 9] },
                { key: '3', size: '20', start: '14' },
            ]);
        });
});
