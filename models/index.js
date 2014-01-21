var Sequelize = require('sequelize');

var sequelize = 
  module.exports.sequelize = 
  new Sequelize('test', 'root', 'root', {
    host: '127.0.0.1'
  });

module.exports.PerOneMinuteEnergyConsumptions = sequelize.define('per_one_minute_energy_consumptions', {
  device_id: {
    type: Sequelize.INTEGER.UNSIGNED,
    validate: {
      notNull: true
    }
  },
  kwh: Sequelize.FLOAT
});

module.exports.EnergyConsumptions = sequelize.define('energy_consumptions', {
  device_id: {
    type: Sequelize.INTEGER.UNSIGNED,
    validate: {
      notNull: true
    }
  },
  kw: Sequelize.FLOAT,
  kwh: Sequelize.FLOAT,
}, {
  define: {
    freezeTableName: true
  }
});