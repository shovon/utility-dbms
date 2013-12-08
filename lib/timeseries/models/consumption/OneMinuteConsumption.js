var OneMinuteConsumption = sequelize.define(
  'consumption_1m',
  attributes.modelAttributes,
  attributes.modelOptions
);

OneMinuteConsumption.createFromTime = function (timestamp) {
  throw new Error('Not yet implemented');
};