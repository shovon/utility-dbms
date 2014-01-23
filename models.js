var Sequelize = require('sequelize');
var bluebird = require('bluebird');
var async = require('async');
var numbers = require('numbers');
var _ = require('lodash');

module.exports.ValidationErrors = ValidationErrors;
function ValidationErrors(err) {
  var finalMessage = [];
  Object.keys(err).forEach(function (key) {
    finalMessage.push(key + ': ' + err[key]);
  });
  this.message = finalMessage.join('\n');
}
ValidationErrors.prototype = Error.prototype;

var sequelize = 
  module.exports.sequelize = 
  new Sequelize('test', 'root', 'root', {
    host: '127.0.0.1'
  });

// TODO: unit test this.
function roundTime(date, coeff) {
  return new Date(Math.floor(date.getTime() / coeff) * coeff);
}

var ONE_MINUTE = 1000 * 60;

var granularityCommon = {
  time: {
    type: Sequelize.DATE,
    notNull: true
  },
  kwh_sum: {
    type: Sequelize.FLOAT,
    defaultValue: 0
  },
  kwh_average: {
    type: Sequelize.FLOAT,
    defaultValue: 0
  },
  kwh_median: Sequelize.FLOAT,
  kwh_min: Sequelize.FLOAT,
  kwh_max: Sequelize.FLOAT,
  kwh_first_quartile: Sequelize.FLOAT,
  kwh_third_quartile: Sequelize.FLOAT,
  kwh_standard_deviation: Sequelize.FLOAT
};

var consumptionCommon = {
  time: {
    type: Sequelize.DATE,
    validate: {
      notNull: true
    }
  },

  kw: {
    type: Sequelize.FLOAT,
    defaultValue: 0
  },
  kwh: {
    type: Sequelize.FLOAT,
    defaultValue: 0
  },
  kwh_difference: {
    type: Sequelize.FLOAT,
    defaultValue: 0
  }
};

function createCollector(interval) {
  return function (granularModel, time, device_id) {
    var self = this;

    var rounded = roundTime(time, interval);

    var def = bluebird.defer();
    var promise = def.promise;

    promise.success = function (fn) {
      return promise.then(fn);
    };

    promise.error = function (fn) {
      return promise.then(function () {}, fn);
    };

    granularModel.model.findAll({
      where: [
        'time > ? && time <= ? && device_id = ?',
        rounded,
        time,
        device_id
      ]
    }).success(function (consumptions) {
      var statistics = {
        kwh: 0,
        kwh_average: 0
      };
      if (consumptions.length) {
        var kwhs = consumptions.map(function (consumption) {
          return consumption.values[granularModel.readingsPropertyName];
        });
        statistics.kwh_sum = kwhs.reduce(function (prev, curr) {
          return prev + curr;
        });
        var report = numbers.statistic.report(kwhs);
        statistics.kwh_average = report.mean;
        statistics.kwh_median = report.median;
        statistics.kwh_first_quartile = report.firstQuartile;
        statistics.kwh_third_quartile = report.thirdQuartile;
        statistics.kwh_min = kwhs.slice().sort()[0];
        statistics.kwh_max = kwhs.slice().sort()[kwhs.length - 1];
        statistics.kwh_standard_deviation = report.standardDev;
      }

      self.find({
        order: 'time DESC',
        where: [ 'device_id = ?', device_id ]
      }).success(function (minuteData) {
        if (
            !minuteData ||
            // For some odd reason, the queried values do not correspond
            // with the columns defined in the database schema. Hence why
            // I'm omitting the `.getTime()` call from
            // `minuteData.values.time`.
            rounded.getTime() !==
              roundTime(minuteData.values.time, interval).getTime()
        ) {
          self.create(
            _.assign(
              {
                time: roundTime(time, interval),
                device_id: device_id
              },
              statistics
            )
          )
          .success(function (minuteData) {
            def.resolve(minuteData);
          }).error(function (err) {
            def.reject(err);
          });
          return
        }

        _.assign(minuteData.values, statistics);

        minuteData.save().success(function (minuteData) {
          def.resolve(minuteData);
        }).error(function (err) {
          def.reject(err);
        });
      }).error(function (err) {
        def.reject(err);
      });
    }).error(function (err) {
      def.reject(err);
    });

    return promise;
  };
}

function createTotalsCollector(interval) {
  return function (granularModel, time) {
    var self = this;

    // TODO: soft code the `ONE_MINUTE`. Instead of relying on the
    //   constant, have it be a parameter of some sort.
    var rounded = roundTime(time, interval);

    var def = bluebird.defer();
    var promise = def.promise;

    promise.success = function (fn) {
      return promise.then(fn);
    };

    promise.error = function (fn) {
      return promise.then(function () {}, fn);
    };

    granularModel.model.findAll({
      where: [
        'time > ? && time <= ?',
        rounded,
        time
      ]
    })
    .success(function (consumptions) {
      var statistics = {
        kwh: 0,
        kwh_average: 0
      };
      if (consumptions.length) {
        var kwhs = consumptions.map(function (consumption) {
          return consumption.values[granularModel.readingsPropertyName];
        });
        statistics.kwh_sum = kwhs.reduce(function (prev, curr) {
          return prev + curr;
        });
        var report = numbers.statistic.report(kwhs);
        statistics.kwh_average = report.mean;
        statistics.kwh_median = report.median;
        statistics.kwh_first_quartile = report.firstQuartile;
        statistics.kwh_third_quartile = report.thirdQuartile;
        statistics.kwh_min = kwhs.slice().sort()[0];
        statistics.kwh_max = kwhs.slice().sort()[kwhs.length - 1];
        statistics.kwh_standard_deviation = report.standardDev;
      }

      self.find({
        order: 'time DESC'
      })
      .success(function (minuteData) {
        if (
          !minuteData ||
          rounded.getTime() !==
            roundTime(minuteData.values.time, interval).getTime()
        ) {
          self.create(
            _.assign(
              {
                time: roundTime(time, interval)
              },
              statistics
            )
          )
          .success(function () {
            def.resolve(minuteData);
          })
          .error(function (err) {
            def.reject(err);
          });
          return;
        }

        _.assign(minuteData.values, statistics);

        minuteData.save().success(function (minuteData) {
          def.resolve(minuteData);
        })
        .error(function (err) {
          def.reject(err);
        })
      })
      .error(function (err) {
        def.reject(err);
      });
    })
    .error(function (err) {
      def.reject(err);
    });

    return promise;
  }
}

var OneMinuteEnergyConsumptionsTotals =
  module.exports.OneMinuteEnergyConsumptionsTotals =
    sequelize.define(
      'energy_consumptions_totals_1m',
      _.assign({}, granularityCommon), {
        freezeTableName: true,
        timestamps: false,
        classMethods: {
          // TODO: soft code this.
          collectRecent: createTotalsCollector(ONE_MINUTE)
        }
      }
    );

var OneMinuteEnergyConsumptions = module.exports.OneMinuteEnergyConsumptions =
  sequelize.define('energy_consumptions_1m', _.assign({
    device_id: {
      type: Sequelize.INTEGER.UNSIGNED,
      validate: {
        notNull: true
      }
    },
  }, granularityCommon), {
    freezeTableName: true,
    timestamps: false,
    classMethods: {
      // TODO: unit test any errors that occur
      collectRecent: createCollector(ONE_MINUTE)
    }
  });

var EnergyConsumptionsTotals = module.exports.EnergyConsumptionsTotals =
  sequelize.define(
    'energy_consumptions_totals',
      _.assign({}, consumptionCommon), {
      freezeTableName: true,
      timestamps: false,
      hooks: {
        beforeValidate: function (consumption, callback) {
          var self = this;

          this.find({
            order: 'time DESC'
          })
          .success(function (prev) {
            if (prev) {
              if (prev.values.time > consumption.values.time) {
                var err = new Error(
                  'Current time: ' + consumption.values.time + '\n' +
                  'Previous time: ' + prev.values.time + '\n\n' +
                  'Current time must be greater than previous time'
                );
                return callback(err);
              }
              consumption.values.kwh_difference =
                consumption.values.kwh - prev.values.kwh;
            } else {
              consumption.values.kwh_difference = consumption.values.kwh;
            }

            callback(null, consumption);
          })
          .error(callback);
        },
        afterCreate: function (consumption, callback) {
          OneMinuteEnergyConsumptionsTotals.collectRecent(
            {
              model: this,
              readingsPropertyName: 'kwh_difference'
            },
            consumption.values.time
          )
          .success(function () {
            callback(null, consumption);
          })
          .error(callback);
        }
      }
    }
  )

var EnergyConsumptions = module.exports.EnergyConsumptions =
  sequelize.define('energy_consumptions', _.assign({
    device_id: {
      type: Sequelize.INTEGER.UNSIGNED,
      validate: {
        notNull: true
      }
    },
  }, consumptionCommon), {
    freezeTableName: true,
    timestamps: false,
    hooks: {
      beforeValidate: function (consumption, callback) {
        var self = this;

        // Look for the most recent entry.
        this.find({
          where: [ 'device_id = ?', consumption.values.device_id ],
          order: 'time DESC' })
        .success(function (prev) {
          if (prev) {
            // We want our data to be inserted in chronological order. Throw
            // an error if anything screws up.
            if (prev.values.time > consumption.values.time) {
              var err = new Error(
                'Current time: ' + consumption.values.time + '\n' +
                'Previous time: ' + prev.values.time + '\n\n' +
                'Current time must be greater than previous time'
              );
              return callback(err);
            }
            consumption.values.kwh_difference =
              consumption.values.kwh - prev.values.kwh;
          } else {
            consumption.values.kwh_difference = consumption.values.kwh
          }

          callback(null, consumption);
        }).error(callback);
      },
      afterCreate: function (consumption, callback) {
        OneMinuteEnergyConsumptions.collectRecent(
          {
            model: this,
            readingsPropertyName: 'kwh_difference'
          }, 
          consumption.values.time,
          consumption.values.device_id
        )
        .success(function () {
          callback(null, consumption);
        })
        .error(callback);
      }
    }
  });

// Override the `bulkCreate` static method. And because this method is being
// overridden, it may mean that bugs may arise. So far, there doesn't seem to
// be any, so let's keep this overridden.
EnergyConsumptions.bulkCreate = function (data) {
  var self = this;
  var def = bluebird.defer();
  async.each(data.devices, function (con, callback) {
    var con = _.assign({
      time: data.time
    }, con);
    EnergyConsumptions.create(con).success(function (con) {
      callback(null, con);
    }).error(function (err) {
      if (err instanceof Error) {
        return callback(err);
      }
      callback(new ValidationErrors(err));
    });
  }, function (err, consumptions) {
    if (err) {
      return def.reject(err);
    }

    var totals = _.assign(
      data.devices.map(function (data) {
        return {
          kw: data.kw,
          kwh: data.kwh
        };
      })
      .reduce(function (prev, curr) {
        return {
          kw: prev.kw + curr.kw,
          kwh: prev.kwh + curr.kwh
        };
      }),
      {
        time: data.time
      }
    );

    EnergyConsumptionsTotals
    .create(totals)
    .success(function (consumptionTotal) {
      def.resolve({
        consumptions: consumptions,
        consumptionTotal: consumptionTotal
      });
    })
    .error(function (err) {
      def.reject(err);
    });
  });
  var promise = def.promise;
  promise.success = function (fn) {
    return promise.then(fn);
  };
  promise.error = function (fn) {
    return promise.then(function () {}, fn);
  };
  return promise;
};
