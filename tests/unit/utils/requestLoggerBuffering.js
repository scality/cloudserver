const assert = require('assert');

const { makeRealRequestLogger, bufferedEntryCount } = require('../helpers');
// lib/utilities/logger initializes Config on load; the unit environment
// (CI=true, S3BACKEND=mem) is set before mocha loads any test file, so this
// is safe at the top level, same as every test that pulls in helpers.
const serverLogger = require('../../../lib/utilities/logger');

/**
 * Pins the werelogs RequestLogger behaviour that the no-restricted-syntax rule
 * in eslint.config.mjs exists to protect against.
 *
 * A RequestLogger keeps every entry it is handed and only flushes when
 * something logs at or above the dump threshold. That is deliberate - it lets
 * an error carry the whole backstory of its request - and it is safe precisely
 * because the logger is discarded when the request ends.
 *
 * Store one on an object that outlives the request and the buffer grows until
 * the process restarts, and a single error-level write then dumps all of it
 * at once. If werelogs ever changes this contract, these assertions are how
 * we find out.
 */
describe('werelogs RequestLogger buffering', () => {
    it('should buffer entries that are below the log level and never emitted', () => {
        const log = makeRealRequestLogger({ level: 'info', dump: 'error' });

        for (let i = 0; i < 100; i++) {
            log.debug('background chatter', { i });
        }

        assert.strictEqual(bufferedEntryCount(log), 100,
            'debug entries are retained even though logLevel info drops them from output');
    });

    it('should keep buffering without bound while nothing reaches the dump threshold', () => {
        const log = makeRealRequestLogger({ level: 'info', dump: 'error' });

        log.debug('one');
        log.trace('two');
        log.info('three');
        log.warn('four');

        assert.strictEqual(bufferedEntryCount(log), 4,
            'trace through warn are all buffered; only error drains');
    });

    it('should flush the whole buffer on a single error-level write', () => {
        const log = makeRealRequestLogger({ level: 'info', dump: 'error' });

        for (let i = 0; i < 100; i++) {
            log.debug('background chatter', { i });
        }
        assert.strictEqual(bufferedEntryCount(log), 100);

        log.error('something failed');

        assert.strictEqual(bufferedEntryCount(log), 0,
            'one error emits every buffered entry at once and empties the buffer');
    });

    it('should give the plain server logger no buffer at all', () => {
        // a werelogs Logger, not a RequestLogger: it writes through and drops
        // sub-level entries. This is what long-lived objects must use.
        assert.strictEqual(serverLogger.entries, undefined,
            'the server logger must not accumulate entries');
    });
});
