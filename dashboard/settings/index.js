const nconf = require('nconf');
const path = require('path');

nconf.use('memory');
nconf.set('environment', process.env.NODE_ENV || 'production');

nconf.file(path.join(__dirname, nconf.get('environment') + '.json'));

module.exports.get = function (key) {
  return nconf.get(key);
};