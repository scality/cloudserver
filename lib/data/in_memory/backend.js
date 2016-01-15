const ds = [];
let count = 0;

const backend = {
    put: function putMem(value, keyContext, callback) {
        ds[count] = { value, keyContext };
        callback(null, [ count++ ]);
    },

    get: function getMem(keys, callback) {
        callback(null, keys.map(key => ds[key].value));
    },

    delete: function delMem(keys, callback) {
        keys.forEach(key => { delete ds[key]; });
        callback(null);
    }
};

export default backend;
