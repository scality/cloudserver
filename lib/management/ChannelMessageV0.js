/**
 * Target service that should handle a message
 * @readonly
 * @enum {number}
 */
const MessageType = {
    /** Message that contains a configuration overlay */
    CONFIG_OVERLAY_MESSAGE: 1,
    /** Message that requests a metrics report */
    METRICS_REQUEST_MESSAGE: 2,
    /** Message that contains a metrics report */
    METRICS_REPORT_MESSAGE: 3,
    /** Close the virtual TCP socket associated to the channel */
    CHANNEL_CLOSE_MESSAGE: 4,
    /** Write data to the virtual TCP socket associated to the channel */
    CHANNEL_PAYLOAD_MESSAGE: 5,
};

/**
 * Target service that should handle a message
 * @readonly
 * @enum {number}
 */
const TargetType = {
    /** Let the dispatcher choose the most appropriate message */
    TARGET_ANY: 0,
};

const headerSize = 3;

class ChannelMessageV0 {
    /**
     * @param  {Buffer} buffer Message bytes
     */
    constructor(buffer) {
        this.messageType = buffer.readUInt8(0);
        this.channelNumber = buffer.readUInt8(1);
        this.target = buffer.readUInt8(2);
        this.payload = buffer.slice(headerSize);
    }

    /**
     * @returns {number} Message type
     */
    getType() {
        return this.messageType;
    }

    /**
     * @returns {number} Channel number if applicable
     */
    getChannelNumber() {
        return this.channelNumber;
    }

    /**
     * @returns {number} Target service, or 0 to choose automatically
     */
    getTarget() {
        return this.target;
    }

    /**
     * @returns {Buffer} Message payload if applicable
     */
    getPayload() {
        return this.payload;
    }

    /**
     * Creates a wire representation of a channel close message
     *
     * @param  {number} channelId Channel number
     *
     * @returns {Buffer} wire representation
     */
    static encodeChannelCloseMessage(channelId) {
        const buf = Buffer.alloc(headerSize);
        buf.writeUInt8(MessageType.CHANNEL_CLOSE_MESSAGE, 0);
        buf.writeUInt8(channelId, 1);
        buf.writeUInt8(TargetType.TARGET_ANY, 2);
        return buf;
    }

    /**
     * Creates a wire representation of a channel data message
     *
     * @param  {number} channelId Channel number
     * @param  {Buffer} data Payload
     *
     * @returns {Buffer} wire representation
     */
    static encodeChannelDataMessage(channelId, data) {
        const buf = Buffer.alloc(data.length + headerSize);
        buf.writeUInt8(MessageType.CHANNEL_PAYLOAD_MESSAGE, 0);
        buf.writeUInt8(channelId, 1);
        buf.writeUInt8(TargetType.TARGET_ANY, 2);
        data.copy(buf, headerSize);
        return buf;
    }

    /**
     * Creates a wire representation of a metrics message
     *
     * @param  {object} body Metrics report
     *
     * @returns {Buffer} wire representation
     */
    static encodeMetricsReportMessage(body) {
        const report = JSON.stringify(body);
        const buf = Buffer.alloc(report.length + headerSize);
        buf.writeUInt8(MessageType.METRICS_REPORT_MESSAGE, 0);
        buf.writeUInt8(0, 1);
        buf.writeUInt8(TargetType.TARGET_ANY, 2);
        buf.write(report, headerSize);
        return buf;
    }

    /**
     * Protocol name used for subprotocol negociation
     */
    static get protocolName() {
        return 'zenko-secure-channel-v0';
    }
}

module.exports = {
    ChannelMessageV0,
    MessageType,
    TargetType,
};
