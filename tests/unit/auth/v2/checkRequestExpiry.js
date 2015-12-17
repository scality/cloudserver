import { expect } from 'chai';
import checkRequestExpiry from '../../../../lib/auth/v2/checkRequestExpiry';

describe('checkTimestamp for timecheck in header auth', () => {
    it('should return true if the date in the header is ' +
       'more than 15 minutes old', () => {
        let timeStamp = 'Mon Sep 21 2015 17:12:58 GMT-0700 (PDT)';
        timeStamp = Date.parse(timeStamp);
        const result = checkRequestExpiry(timeStamp);
        expect(result).to.be.true;
    });

    it('should return true if the date in the header is more ' +
       'than 15 minutes in the future', () => {
        // Note: This test will have to be updated in 2095
        let timeStamp = 'Mon Sep 25 2095 17:12:58 GMT-0700 (PDT)';
        timeStamp = Date.parse(timeStamp);
        const result = checkRequestExpiry(timeStamp);
        expect(result).to.be.true;
    });

    it('should return false if the date in the header is ' +
       'within 15 minutes of current time', () => {
        const timeStamp = new Date();
        const result = checkRequestExpiry(timeStamp);
        expect(result).to.be.false;
    });
});
