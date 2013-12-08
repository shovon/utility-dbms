var databaseConfig = require('../config/config.json');

var config = {
  environment: process.env.NODE_ENV || 'development'
};

config.database = databaseConfig[config.environment];

module.exports = config;