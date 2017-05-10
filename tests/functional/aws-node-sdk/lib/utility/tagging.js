function _generateWordWithLength(nbr) {
    let key = '';
    for (let i = 0; i < nbr; i++) {
        key += 'a';
    }
    return key;
}

export const taggingTests = [
    { tag: { key: '+- =._:/', value: '+- =._:/' },
      it: 'should return tags if tags are valid' },
    { tag: { key: 'key1', value: '' },
      it: 'should return tags if value is an empty string' },
    { tag: { key: _generateWordWithLength(129), value: 'foo' },
      error: 'InvalidTag',
      it: 'should return InvalidTag if key length is greater than 128' },
    { tag: { key: 'bar', value: _generateWordWithLength(257) },
      error: 'InvalidTag',
      it: 'should return InvalidTag if key length is greater than 256',
    },
    { tag: { key: 'bar$', value: 'foo' },
      error: 'InvalidTag',
      it: 'should return InvalidTag if invalid key',
    },
    { tag: { key: 'bar', value: 'foo#' },
      error: 'InvalidTag',
      it: 'should return InvalidTag if invalid value',
    },
];
