module.exports = {

    markerFilter: function (marker, array) {
        var length = array.length;
        for (var i = 0; i < length; i++) {
            if (marker >= array[i]) {
                array.shift();
                i--;
            } else {
                break;
            }
        }
        return array;
    },

    prefixFilter: function (prefix, array) {
        for (var i = 0; i < array.length; i++) {
            if (array[i].indexOf(prefix) !== 0) {
                array.splice(i, 1);
                i--;
            }
        }
        return array;
    },

    findNextMarker: function (currentIndex, array, responseObject) {
        var NextMarker, delimited_key;
        var length = array.length;
        // Iterate through remainder of keys array to find next key that would NOT get rolled up into an existing CommonPrefix
        for (var i = currentIndex; i < length; i++) {
            var current_key = array[i];
            if (responseObject.Delimiter) {
                var delimiter_index = current_key.indexOf(responseObject.Delimiter);
                delimited_key = current_key.slice(0, delimiter_index + 1);
            }
            if (!responseObject.hasCommonPrefix(delimited_key)) {
                NextMarker = current_key;
                break;
            }
        }
        return NextMarker;
    },

    isKeyInContents: function (responseObject, key) {
        var length = responseObject.Contents.length;
        for (var i = 0; i < length; i++) {
            if (responseObject.Contents[i].Key === key) {
                return true;
            }
        }
        return false;
    },

};
