const ds = [];
let count = 0;

const backend = {
    put: function putMem(value, keyContext, reqUids, callback) {
        ds[count] = { value, keyContext };
        callback(null, [ count++ ]);
    },

    get: function getMem(keys, reqUids, callback) {
        callback(null, keys.map(key => ds[key].value));
    },

    delete: function delMem(keys, reqUids, callback) {
        keys.forEach(key => { delete ds[key]; });
        callback(null);
    }
};

export default backend;
