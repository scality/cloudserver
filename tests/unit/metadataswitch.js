const wrapper = require('../../lib/metadata/wrapper');
const backend = require('arsenal').storage.metadata.inMemory.metastore;

wrapper.switch(backend, () => {});

module.exports = wrapper;
