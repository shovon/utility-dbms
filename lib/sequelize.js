var Sequelize = require('sequelize');
var config = require('./config');

var sequelize = new Sequelize(
  config.database.database,
  config.database.username,
  config.database.password, {
    host: config.database.host,
    port: config.database.port || 3306
  }
);

module.exports = sequelize;