import Logger from 'werelogs';

import Config from '../Config';

const _config = new Config();

export const logger = new Logger('S3', {
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});
