import nconf from 'nconf';
import path from 'path';

nconf.use('memory');
nconf.set('environment', process.env.NODE_ENV || 'production');

nconf.file(path.join(__dirname, nconf.get('environment') + '.json'));

export function get(key) {
  return nconf.get(key);
}
