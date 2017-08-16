class ListResult {
    constructor() {
        this.IsTruncated = false;
        this.NextMarker = undefined;
        this.CommonPrefixes = [];
        /*
        Note:  this.MaxKeys will get incremented as
        keys are added so that when response is returned,
        this.MaxKeys will equal total keys in response
        (with each CommonPrefix counting as 1 key)
        */
        this.MaxKeys = 0;
    }

    addCommonPrefix(prefix) {
        if (!this.hasCommonPrefix(prefix)) {
            this.CommonPrefixes.push(prefix);
            this.MaxKeys += 1;
        }
    }

    hasCommonPrefix(prefix) {
        return (this.CommonPrefixes.indexOf(prefix) !== -1);
    }
}

module.exports = ListResult;
