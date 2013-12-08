var RawConsumption = require('./models/consumption/RawConsumption');
var highestGranularity = require('./models/consumption/highestGranularity');

module.exports.insert = function (params) {
  return RawConsumption.find({
    order: 'timestamp DESC',
    limit: 1
  }).then(function (result) {
    var values = {
      timestamp: params.timestamp,
      kw: params.kw,
      kwh: params.kwh
    };

    if (result.count) {
      values.kwh_difference = params.kwh - result.row[0].kwh;
    }
    return RawConsumption.create(values).then(function () {
      return highestGranularity.createFromTime(values.timestamp);
    });
  });
};