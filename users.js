const Datastore = require('nedb');

module.exports = new Datastore({ filename: './.db/users', autoload: true });