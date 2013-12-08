var Sequelize = require('sequelize');

var RawConsumption = sequelize.define('raw_consumption', {
  timestamp: {
    type: Sequelize.DATE,
    allowNull: false
  },
  kw: {
    type: Sequelize.FLOAT,
    allowNull: false,
  },
  kwh: {
    type: Sequelize.FLOAT,
    allowNull: false
  },
  kwh_difference: {
    type: Sequelize.FLOAT
  }
}, {
  timestamps: false,
  freezeTableName: true
});