const assert = require('assert');

const { ChannelMessageV0, MessageType, TargetType } = require('../../../lib/management/ChannelMessageV0');

const { CONFIG_OVERLAY_MESSAGE, METRICS_REQUEST_MESSAGE, METRICS_REPORT_MESSAGE } = MessageType;

const { TARGET_ANY } = TargetType;

describe('ChannelMessageV0', () => {
    describe('codec', () => {
        it('should roundtrip metrics report', () => {
            const b = ChannelMessageV0.encodeMetricsReportMessage({ a: 1 });
            const m = new ChannelMessageV0(b);

            assert.strictEqual(METRICS_REPORT_MESSAGE, m.getType());
            assert.strictEqual(0, m.getChannelNumber());
            assert.strictEqual(m.getTarget(), TARGET_ANY);
            assert.strictEqual(m.getPayload().toString(), '{"a":1}');
        });
    });

    describe('decoder', () => {
        it('should parse metrics request', () => {
            const b = Buffer.from([METRICS_REQUEST_MESSAGE, 0, 0]);
            const m = new ChannelMessageV0(b);

            assert.strictEqual(METRICS_REQUEST_MESSAGE, m.getType());
            assert.strictEqual(0, m.getChannelNumber());
            assert.strictEqual(m.getTarget(), TARGET_ANY);
        });

        it('should parse overlay push', () => {
            const b = Buffer.from([CONFIG_OVERLAY_MESSAGE, 0, 0, 34, 65, 34]);
            const m = new ChannelMessageV0(b);

            assert.strictEqual(CONFIG_OVERLAY_MESSAGE, m.getType());
            assert.strictEqual(0, m.getChannelNumber());
            assert.strictEqual(m.getTarget(), TARGET_ANY);
            assert.strictEqual(m.getPayload().toString(), '"A"');
        });
    });
});
