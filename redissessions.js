const RedisSessions = require('redis-sessions');
const settings = require('./settings');

module.exports.rs = new RedisSessions(settings.get('redis'));
module.exports.rsapp = 'dbms';