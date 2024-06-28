const { ParentBasedSampler, TraceIdRatioBasedSampler, SamplingDecision } = require('@opentelemetry/sdk-trace-base');

class HealthcheckExcludingSampler {
    constructor(sampler, excludeHealthcheck) {
        this._sampler = sampler;
        this._excludeHealthcheck = excludeHealthcheck;
    }

    shouldSample(context, traceId, spanName, spanKind, attributes, links) {
        const url = attributes['http.url'] || '';
        if (this._excludeHealthcheck && url.includes('healthcheck')) {
            return { decision: SamplingDecision.NOT_RECORD };
        }
        return this._sampler.shouldSample(context, traceId, spanName, spanKind, attributes, links);
    }

    toString() {
        return `HealthcheckExcludingSampler{${this._sampler.toString()}}`;
    }
}

function createSampler(samplingRatio, excludeHealthcheck) {
    return new ParentBasedSampler({
        root: new TraceIdRatioBasedSampler(samplingRatio),
    });
}

module.exports = { createSampler };
