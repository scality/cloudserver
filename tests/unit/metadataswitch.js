const wrapper = require('../../lib/metadata/wrapper');
const backend = require('armory').storage.metadata.inMemory.metastore;

wrapper.switch(backend, () => {});

module.exports = wrapper;
