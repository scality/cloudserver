class Cache {
    constructor() {
        this.lastChecked = null;
        this.result = null;
    }

    /**
     * Retrieves the cached result with the last checked timestamp.
     * @returns {object|null} An object containing the result and lastChecked, or null if not set.
     */
    getResult() {
        if (!this.result) {
            return null;
        }

        return Object.assign({}, this.result, {
            lastChecked: this.lastChecked ? new Date(this.lastChecked).toISOString() : null,
        });
    }

    /**
     * Retrieves the last checked timestamp.
     * @returns {number|null} The timestamp of the last check or null if never checked.
     */
    getLastChecked() {
        return this.lastChecked;
    }

    /**
     * Updates the cache with a new result and timestamp.
     * @param {object} result - The result to cache.
     * @returns {undefined}
     */
    setResult(result) {
        this.lastChecked = Date.now();
        this.result = result;
    }

    /**
     * Determines if the cache should be refreshed based on the last checked time.
     * @param {number} duration - Duration in milliseconds for cache validity.
     * @returns {boolean} true if the cache should be refreshed, else false.
     */
    shouldRefresh(duration = 1 * 60 * 60 * 1000) { // Default: 1 hour
        if (!this.lastChecked) {
            return true;
        }

        const now = Date.now();
        const elapsed = now - this.lastChecked;
        const jitter = Math.floor(Math.random() * 15 * 60 * 1000); // Up to 15 minutes
        return elapsed > (duration - jitter);
    }

    /**
     * Clears the cache.
     * @returns {undefined}
     */
    clear() {
        this.lastChecked = null;
        this.result = null;
    }
}

module.exports = Cache;
