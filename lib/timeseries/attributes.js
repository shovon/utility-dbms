var Sequelize = require('sequelize');

module.exports.modelAttributes = {
  device_id: {
    type: Sequelize.INTEGER,
    allowNull: false
  },
  timestamp: {
    type: Sequelize.DATE,
    allowNull: false
  },
  kw: {
    type: Sequelize.FLOAT,
    allowNull: false
  },
  kwh: {
    type: Sequelize.FLOAT,
    allowNull: false
  },
  min: {
    type: Sequelize.FLOAT,
    allowNull: false
  },
  max: {
    type: Sequelize.FLOAT,
    allowNull: false
  }
};

module.exports.modelOptions = {
  timestamps: false,
  freezeTableName: true
};