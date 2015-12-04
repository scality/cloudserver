const ds = [];
let count = 0;

const backend = {
    put: function putMem(value, callback) {
        ds[count] = value;
        callback(null, [ count++ ]);
    },

    get: function getMem(keys, callback) {
        callback(null, keys.map(key => ds[key]));
    },

    delete: function delMem(keys, callback) {
        keys.forEach(key => { delete ds[key]; });
        callback(null);
    }
};

export default backend;
