var chai = require("chai");
var expect = chai.expect;
var Auth = require("../lib/auth/checkAuth.js");



describe('canonicalization', function () {

    it('should construct a canonicalized header', function () {
        var LOWERCASEHEADERS = {
            date: 'Mon, 21 Sep 2015 22:29:27 GMT',
            'x-amz-request-payer': 'requester',
            authorization: 'AWS accessKey1:V8g5UJUFmMzruMqUHVT6ZwvUw+M=',
            host: 's3.amazonaws.com:80',
            connection: 'Keep-Alive',
            'user-agent': 'Cyberduck/4.7.2.18004 (Mac OS X/10.10.5) (x86_64)' };

        var canonicalizedHeader = Auth._getCanonicalizedAmzHeaders(LOWERCASEHEADERS);

        expect(canonicalizedHeader).to.equal("x-amz-request-payer:requester" + "\n");

    });

    it('should return an empty string as the canonicalized header if no amz headers', function () {
        var LOWERCASEHEADERS = {
            date: 'Mon, 21 Sep 2015 22:29:27 GMT',
            authorization: 'AWS accessKey1:V8g5UJUFmMzruMqUHVT6ZwvUw+M=',
            host: 's3.amazonaws.com:80',
            connection: 'Keep-Alive',
            'user-agent': 'Cyberduck/4.7.2.18004 (Mac OS X/10.10.5) (x86_64)' };

        var canonicalizedHeader = Auth._getCanonicalizedAmzHeaders(LOWERCASEHEADERS);

        expect(canonicalizedHeader).to.equal('');
    });

    it('should construct a canonicalized resource', function () {
        var REQUEST = {
            headers: {
                host: 'bucket.s3.amazonaws.com:80'
            },
            lowerCaseHeaders: {
                host: 'bucket.s3.amazonaws.com:80'
            },
            url: '/obj',
            query: {
                'requestPayment':'yes,please',
                'ignore': 'me'}
        };
        var canonicalizedResource = Auth._getCanonicalizedResource(REQUEST);
        expect(canonicalizedResource).to.equal('bucket/obj?requestPayment=yes,please');
    });

    it('should return the path as the canonicalized resource if no bucket name, \
            overriding headers or delete query', function () {
                var REQUEST = {
                    headers: {
                        host: 's3.amazonaws.com:80'
                    },
                    lowerCaseHeaders: {
                        host: 's3.amazonaws.com:80'
                    },
                    url: '/',
                    query: {
                        'ignore': 'me'}
                };
                var canonicalizedResource = Auth._getCanonicalizedResource(REQUEST);
                expect(canonicalizedResource).to.equal('/');
            });

});


describe("Auth._reconstructSignature",function () {


    it("should reconstruct the signature for a GET request from s3-curl", function () {
        // Based on s3-curl run
        var REQUEST = {
            method: "GET",
            headers: { host: 's3.amazonaws.com',
                'user-agent': 'curl/7.43.0',
                accept: '*/*',
                            date: 'Fri, 18 Sep 2015 22:57:23 +0000',
                            authorization: 'AWS accessKey1:MJNF7AqNapSu32TlBOVkcAxj58c=' },
            url: "/bucket",
            lowerCaseHeaders: {
                                date: 'Fri, 18 Sep 2015 22:57:23 +0000',
                            },
            query: {}
        };
        var SECRET_KEY = "verySecretKey1";

        var reconstructedSig = Auth._reconstructSignature(SECRET_KEY, REQUEST);
        expect(reconstructedSig).to.equal("MJNF7AqNapSu32TlBOVkcAxj58c=");
    });


    it("should reconstruct the signature for a GET request from CyberDuck", function () {
            // Based on CyberDuck request
                                var REQUEST = {
                method: "GET",
                headers: { date: 'Mon, 21 Sep 2015 22:29:27 GMT',
            'x-amz-request-payer': 'requester',
            authorization: 'AWS accessKey1:V8g5UJUFmMzruMqUHVT6ZwvUw+M=',
            host: 's3.amazonaws.com:80',
            connection: 'Keep-Alive',
            'user-agent': 'Cyberduck/4.7.2.18004 (Mac OS X/10.10.5) (x86_64)' },
                lowerCaseHeaders: { date: 'Mon, 21 Sep 2015 22:29:27 GMT',
            'x-amz-request-payer': 'requester',
            authorization: 'AWS accessKey1:V8g5UJUFmMzruMqUHVT6ZwvUw+M=',
            host: 's3.amazonaws.com:80',
            connection: 'Keep-Alive',
            'user-agent': 'Cyberduck/4.7.2.18004 (Mac OS X/10.10.5) (x86_64)' },
                url: '/mb/?max-keys=1000&prefix&delimiter=%2F',
                query: { 'max-keys': '1000', prefix: '', delimiter: '/' }
            };
                                var SECRET_KEY = "verySecretKey1";

                                var reconstructedSig = Auth._reconstructSignature(SECRET_KEY, REQUEST);
                                expect(reconstructedSig).to.equal("V8g5UJUFmMzruMqUHVT6ZwvUw+M=");
                            });



    it("should reconstruct the signature for a PUT request from s3cmd", function () {
                // Based on s3cmd run
                var REQUEST = {
                    method: "PUT",
                    headers: { host: '127.0.0.1:8000',
                'accept-encoding': 'identity',
                authorization: 'AWS accessKey1:fWPcicKn7Fhzfje/0pRTifCxL44=',
                'content-length': '3941',
                'content-type': 'binary/octet-stream',
                'x-amz-date': 'Fri, 18 Sep 2015 23:32:34 +0000',
                'x-amz-meta-s3cmd-attrs': 'uid:501/gname:staff/uname:lhs/gid:20/mode:33060/mtime:1319136702/atime:1442619138/md5:5e714348185ffe355a76b754f79176d6/ctime:1441840220',
                'x-amz-now': 'susdr',
                'x-amz-y': 'what' },
                    url: "/test/obj",
                    lowerCaseHeaders: { host: '127.0.0.1:8000',
                'accept-encoding': 'identity',
                authorization: 'AWS accessKey1:fWPcicKn7Fhzfje/0pRTifCxL44=',
                'content-length': '3941',
                'content-type': 'binary/octet-stream',
                'x-amz-date': 'Fri, 18 Sep 2015 23:32:34 +0000',
                'x-amz-meta-s3cmd-attrs': 'uid:501/gname:staff/uname:lhs/gid:20/mode:33060/mtime:1319136702/atime:1442619138/md5:5e714348185ffe355a76b754f79176d6/ctime:1441840220',
                'x-amz-now': 'susdr',
                'x-amz-y': 'what' },
                    query: {}

                };
                var SECRET_KEY = "verySecretKey1";

                var reconstructedSig = Auth._reconstructSignature(SECRET_KEY, REQUEST);
                expect(reconstructedSig).to.equal("fWPcicKn7Fhzfje/0pRTifCxL44=");
            });

    describe("Auth._checkTimestamp for timecheck in header auth", function () {

                it("should return true if the date in the header is more than 15 minutes old", function () {
                    var timeStamp = 'Mon Sep 21 2015 17:12:58 GMT-0700 (PDT)';
                    timeStamp = Date.parse(timeStamp);
                    var result = Auth._checkTimestamp(timeStamp);
                    expect(result).to.be.true;
                });

                it("should return true if the date in the header is more than 15 minutes in the future", function () {
                    // Note: This test will have to be updated in 2095
                    var timeStamp = 'Mon Sep 25 2095 17:12:58 GMT-0700 (PDT)';
                    timeStamp = Date.parse(timeStamp);
                    var result = Auth._checkTimestamp(timeStamp);
                    expect(result).to.be.true;
                });

                it("should return false if the date in the header is within 15 minutes of current time", function () {
                    var timeStamp = new Date();
                    var result = Auth._checkTimestamp(timeStamp);
                    expect(result).to.be.false;
                });

            });


    describe('Error handling in checkAuth', function () {

                it('should return an error message if no secret key is associated with access key', function (done) {

                    var DATE = new Date();

                    var REQUEST = {
                        method: "GET",
                        lowerCaseHeaders: { host: 's3.amazonaws.com',
                            'user-agent': 'curl/7.43.0',
                            accept: '*/*',
                                        date: DATE,
                                        authorization: 'AWS brokenKey1:MJNF7AqNapSu32TlBOVkcAxj58c=' },
                        url: "/bucket",
                    };

                    Auth.checkAuth(REQUEST, function (err, success) {
                                            expect(err).to.equal('InvalidAccessKeyId');
                                            done();
                                        });
                });


                it('should return an error message if no date header is provided with v2header auth check', function (done) {

                                            var REQUEST = {
                                            method: "GET",
                                            lowerCaseHeaders: { host: 's3.amazonaws.com',
                                        'user-agent': 'curl/7.43.0',
                                        accept: '*/*',
                                                    authorization: 'AWS accessKey1:MJNF7AqNapSu32TlBOVkcAxj58c=' },
                                            url: "/bucket",
                                        };

                                            Auth.checkAuth(REQUEST, function (err, success) {
                                                        expect(err).to.equal('MissingSecurityHeader');
                                                        done();
                                                    });
                                        });

                it('should return an error message if the Expires query parameter is more than 15 minutes \
                                                    old with query auth check', function (done) {

                                                        var REQUEST = {
                                                        method: "GET",
                                                        url: "/bucket?AWSAccessKeyId=accessKey1&Expires=1141889120&Signature=vjbyPxybdZaNmGa%2ByT272YEAiv4%3D",
                                                        query: { AWSAccessKeyId: 'accessKey1', Expires: '1141889120', Signature: 'vjbyPxybdZaNmGa%2ByT272YEAiv4%3D' },
                                                        lowerCaseHeaders: {},
                                                        headers: {}
                                                    };

                                                        Auth._v2QueryAuthCheck(REQUEST, function (err, success) {
                                                        expect(err).to.equal('RequestTimeTooSkewed');
                                                        done();
                                                    });
                                                    });

                it('should return an error message if the signatures do not match for v2query auth', function (done) {

                    // Date.now() provides milliseconds since 1/1/1970.  AWS Expires is in seconds so need to divide by 1000
                                                        var expires = Date.now() / 1000;
                                                        var fifteenMinutes = (15 * 60);
                                                        expires = expires + fifteenMinutes;

                                                        var REQUEST = {
                        method: "GET",
                        url: `/bucket?AWSAccessKeyId=accessKey1&Expires=${expires}&Signature=vjbyPxybdZaNmGa%2ByT272YEAiv4%3D`,
                        query: { AWSAccessKeyId: 'accessKey1', Expires: expires, Signature: 'vjbyPxybdZaNmGa%2ByT272YEAiv4%3D' },
                        lowerCaseHeaders: {},
                        headers: {}
                    };

                                                        Auth._v2QueryAuthCheck(REQUEST, function (err, success) {
                        expect(err).to.equal('SignatureDoesNotMatch');
                        done();
                    });
                                                    });


                it('should return an error message if the signatures do not match for v2header auth', function (done) {

                        var DATE = new Date();

                        var REQUEST = {
                        method: "GET",
                        lowerCaseHeaders: { host: 's3.amazonaws.com',
                    'user-agent': 'curl/7.43.0',
                    accept: '*/*',
                                date: DATE,
                                authorization: 'AWS accessKey1:MJNF7AqNapSu32TlBOVkcAxj58c=' },
                        url: "/bucket",
                        query: {}
                    };

                        Auth.checkAuth(REQUEST, function (err, success) {
                                    expect(err).to.equal('SignatureDoesNotMatch');
                                    done();
                                });
                    });

            });


});
