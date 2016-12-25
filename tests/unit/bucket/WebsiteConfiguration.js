import assert from 'assert';
import {
    WebsiteConfiguration,
    RoutingRule,
} from '../../../lib/metadata/WebsiteConfiguration';

const testRoutingRuleParams = {
    redirect: {
        protocol: 'http',
        hostName: 'test',
        replaceKeyPrefixWith: '/docs',
        replaceKeyWith: 'cat',
        httpRedirectCode: '303',
    },
    condition: {
        keyPrefixEquals: '/documents',
        httpErrorCodeReturnedEquals: '404',
    },
};

describe('RoutingRule class', () => {
    it('should initialize even if no parameters are provided', done => {
        const routingRule = new RoutingRule();
        assert.strictEqual(routingRule._redirect, undefined);
        assert.strictEqual(routingRule._condition, undefined);
        done();
    });

    it('should return a new routing rule', done => {
        const routingRule = new RoutingRule(testRoutingRuleParams);
        assert.deepStrictEqual(routingRule._redirect,
            testRoutingRuleParams.redirect);
        assert.deepStrictEqual(routingRule._condition,
            testRoutingRuleParams.condition);
        done();
    });

    it('getRedirect should fetch the instance\'s redirect', done => {
        const routingRule = new RoutingRule(testRoutingRuleParams);
        assert.deepStrictEqual(routingRule.getRedirect(),
            testRoutingRuleParams.redirect);
        done();
    });

    it('getCondition should fetch the instance\'s condition', done => {
        const routingRule = new RoutingRule(testRoutingRuleParams);
        assert.deepStrictEqual(routingRule.getCondition(),
            testRoutingRuleParams.condition);
        done();
    });
});

describe('WebsiteConfiguration class', () => {
    it('should initialize even if no parameters are provided', done => {
        const websiteConfig = new WebsiteConfiguration();
        assert.strictEqual(websiteConfig._indexDocument, undefined);
        assert.strictEqual(websiteConfig._errorDocument, undefined);
        assert.strictEqual(websiteConfig._redirectAllRequestsTo, undefined);
        assert.strictEqual(websiteConfig._routingRules, undefined);
        done();
    });

    it('should initialize indexDocument, errorDocument during construction ' +
    'if provided in params', done => {
        const testWebsiteConfigParams = {
            indexDocument: 'index.html',
            errorDocument: 'error.html',
        };
        const websiteConfig = new WebsiteConfiguration(testWebsiteConfigParams);
        assert.strictEqual(websiteConfig._indexDocument, 'index.html');
        assert.strictEqual(websiteConfig._errorDocument, 'error.html');
        done();
    });

    it('should initialize redirectAllRequestsTo during construction if ' +
    'provided in params', done => {
        const testWebsiteConfigParams = {
            redirectAllRequestsTo: {
                hostName: 'test',
                protocol: 'https',
            },
        };
        const websiteConfig = new WebsiteConfiguration(testWebsiteConfigParams);
        assert.strictEqual(websiteConfig._redirectAllRequestsTo.hostName,
            'test');
        assert.strictEqual(websiteConfig._redirectAllRequestsTo.protocol,
            'https');
        done();
    });

    it('should initialize routingRules properly during construction from ' +
    'array of RoutingRule class instances', done => {
        const testWebsiteConfigParams = {
            routingRules: [],
        };
        const testRoutingRule = new RoutingRule(testRoutingRuleParams);
        testWebsiteConfigParams.routingRules.push(testRoutingRule);
        testWebsiteConfigParams.routingRules.push(testRoutingRule);
        testWebsiteConfigParams.routingRules.push(testRoutingRule);
        const websiteConfig = new WebsiteConfiguration(testWebsiteConfigParams);
        assert.deepStrictEqual(websiteConfig._routingRules,
            testWebsiteConfigParams.routingRules);
        done();
    });

    it('should initialize routingRules properly during construction from ' +
    'array of plain objects', done => {
        const testWebsiteConfigParams = {
            routingRules: [],
        };
        testWebsiteConfigParams.routingRules.push(testRoutingRuleParams);
        testWebsiteConfigParams.routingRules.push(testRoutingRuleParams);
        testWebsiteConfigParams.routingRules.push(testRoutingRuleParams);
        const websiteConfig = new WebsiteConfiguration(testWebsiteConfigParams);
        assert.deepEqual(websiteConfig._routingRules[0]._condition,
            testRoutingRuleParams.condition);
        assert.deepEqual(websiteConfig._routingRules[1]._condition,
            testRoutingRuleParams.condition);
        assert.deepEqual(websiteConfig._routingRules[2]._condition,
            testRoutingRuleParams.condition);
        assert.deepEqual(websiteConfig._routingRules[0]._redirect,
            testRoutingRuleParams.redirect);
        assert.deepEqual(websiteConfig._routingRules[1]._redirect,
            testRoutingRuleParams.redirect);
        assert.deepEqual(websiteConfig._routingRules[2]._redirect,
            testRoutingRuleParams.redirect);
        assert(websiteConfig._routingRules[0] instanceof RoutingRule);
        done();
    });

    describe('Getter/setter methods', () => {
        it('for indexDocument should get/set indexDocument property', done => {
            const websiteConfig = new WebsiteConfiguration();
            websiteConfig.setIndexDocument('index.html');
            assert.strictEqual(websiteConfig.getIndexDocument(), 'index.html');
            done();
        });

        it('for errorDocument should get/set errorDocument property', done => {
            const websiteConfig = new WebsiteConfiguration();
            websiteConfig.setErrorDocument('error.html');
            assert.strictEqual(websiteConfig.getErrorDocument(), 'error.html');
            done();
        });

        it('for redirectAllRequestsTo should get/set redirectAllRequestsTo ' +
        'object', done => {
            const websiteConfig = new WebsiteConfiguration();
            const redirectAllRequestsTo = {
                hostName: 'test',
                protocol: 'http',
            };
            websiteConfig.setRedirectAllRequestsTo(redirectAllRequestsTo);
            assert.deepStrictEqual(websiteConfig.getRedirectAllRequestsTo(),
                redirectAllRequestsTo);
            done();
        });

        it('for routingRules should get/set routingRules', done => {
            const websiteConfig = new WebsiteConfiguration();
            const routingRules = [testRoutingRuleParams];
            websiteConfig.setRoutingRules(routingRules);
            assert.strictEqual(websiteConfig.getRoutingRules()[0]._condition,
                routingRules[0].condition);
            assert.strictEqual(websiteConfig.getRoutingRules()[0]._redirect,
                routingRules[0].redirect);
            assert(websiteConfig._routingRules[0] instanceof RoutingRule);
            done();
        });
    });

    it('addRoutingRule should add a RoutingRule to routingRules', done => {
        const websiteConfig = new WebsiteConfiguration();
        websiteConfig.addRoutingRule(testRoutingRuleParams);
        assert(Array.isArray(websiteConfig._routingRules));
        assert.strictEqual(websiteConfig._routingRules.length, 1);
        assert.strictEqual(websiteConfig._routingRules[0].getCondition(),
            testRoutingRuleParams.condition);
        assert.strictEqual(websiteConfig._routingRules[0].getRedirect(),
            testRoutingRuleParams.redirect);
        done();
    });
});
