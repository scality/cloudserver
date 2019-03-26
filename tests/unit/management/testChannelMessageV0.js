const assert = require('assert');

const {
    ChannelMessageV0,
    MessageType,
    TargetType,
} = require('../../../lib/management/ChannelMessageV0');

const {
    CONFIG_OVERLAY_MESSAGE,
    METRICS_REQUEST_MESSAGE,
    METRICS_REPORT_MESSAGE,
    CHANNEL_CLOSE_MESSAGE,
    CHANNEL_PAYLOAD_MESSAGE,
} = MessageType;

const { TARGET_ANY } = TargetType;

describe('ChannelMessageV0', () => {
    describe('codec', () => {
        test('should roundtrip metrics report', () => {
            const b = ChannelMessageV0.encodeMetricsReportMessage({ a: 1 });
            const m = new ChannelMessageV0(b);

            expect(METRICS_REPORT_MESSAGE).toBe(m.getType());
            expect(0).toBe(m.getChannelNumber());
            expect(m.getTarget()).toBe(TARGET_ANY);
            expect(m.getPayload().toString()).toBe('{"a":1}');
        });

        test('should roundtrip channel data', () => {
            const data = new Buffer('dummydata');
            const b = ChannelMessageV0.encodeChannelDataMessage(50, data);
            const m = new ChannelMessageV0(b);

            expect(CHANNEL_PAYLOAD_MESSAGE).toBe(m.getType());
            expect(50).toBe(m.getChannelNumber());
            expect(m.getTarget()).toBe(TARGET_ANY);
            expect(m.getPayload().toString()).toBe('dummydata');
        });

        test('should roundtrip channel close', () => {
            const b = ChannelMessageV0.encodeChannelCloseMessage(3);
            const m = new ChannelMessageV0(b);

            expect(CHANNEL_CLOSE_MESSAGE).toBe(m.getType());
            expect(3).toBe(m.getChannelNumber());
            expect(m.getTarget()).toBe(TARGET_ANY);
        });
    });

    describe('decoder', () => {
        test('should parse metrics request', () => {
            const b = new Buffer([METRICS_REQUEST_MESSAGE, 0, 0]);
            const m = new ChannelMessageV0(b);

            expect(METRICS_REQUEST_MESSAGE).toBe(m.getType());
            expect(0).toBe(m.getChannelNumber());
            expect(m.getTarget()).toBe(TARGET_ANY);
        });

        test('should parse overlay push', () => {
            const b = new Buffer([CONFIG_OVERLAY_MESSAGE, 0, 0, 34, 65, 34]);
            const m = new ChannelMessageV0(b);

            expect(CONFIG_OVERLAY_MESSAGE).toBe(m.getType());
            expect(0).toBe(m.getChannelNumber());
            expect(m.getTarget()).toBe(TARGET_ANY);
            expect(m.getPayload().toString()).toBe('"A"');
        });
    });
});
