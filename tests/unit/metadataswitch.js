import wrapper from '../../lib/metadata/wrapper';
import backend from '../../lib/metadata/in_memory/backend';

wrapper.switch(backend);

export default wrapper;
