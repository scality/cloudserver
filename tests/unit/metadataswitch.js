const wrapper = require('../../lib/metadata/wrapper');
const backend = require('@scality/arsenal').storage.metadata.inMemory.metastore;

wrapper.switch(backend, () => {});

module.exports = wrapper;
