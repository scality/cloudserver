import { Logger } from 'werelogs';

import _config from '../Config';

export const logger = new Logger('S3', {
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});
