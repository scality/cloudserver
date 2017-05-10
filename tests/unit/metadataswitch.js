const wrapper = require('../../lib/metadata/wrapper');
const backend = require('../../lib/metadata/in_memory/backend');

wrapper.switch(backend);

module.exports = wrapper;
