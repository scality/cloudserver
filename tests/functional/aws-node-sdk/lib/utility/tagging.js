const taggingTests = [
    { tag: { key: '+- =._:/', value: '+- =._:/' },
        it: 'should return tags if tags are valid' },
    { tag: { key: 'key1', value: '' },
        it: 'should return tags if value is an empty string' },
    { tag: { key: 'w'.repeat(129), value: 'foo' },
        error: 'InvalidTag',
        it: 'should return InvalidTag if key length is greater than 128' },
    { tag: { key: 'bar', value: 'f'.repeat(257) },
        error: 'InvalidTag',
        it: 'should return InvalidTag if key length is greater than 256',
    },
];

function generateMultipleTagQuery(numberOfTag) {
    let tagsArray = [];
    for (let i = 0; i < numberOfTag; i++) {
        if (i !== 0) {
            and = '&';
        }
        tagsArray.push(`key${i}=value${i}`);
    }
    return tagsArray.join('&');
}

module.exports = {
    taggingTests,
    generateMultipleTagQuery,
};
