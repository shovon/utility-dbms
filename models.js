var Sequelize = require('sequelize');
var bluebird = require('bluebird');
var async = require('async')

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

var OneMinuteEnergyConsumptions = module.exports.OneMinuteEnergyConsumptions =
  sequelize.define('one_minute_energy_consumptions', {
    device_id: {
      type: Sequelize.INTEGER.UNSIGNED,
      validate: {
        notNull: true
      }
    },
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
    kwh_median: {
      type: Sequelize.FLOAT,
      defaultValue: 0
    },
    kwh_standard_deviation: {
      type: Sequelize.FLOAT,
      defaultValue: 0
    },
    kwh_min: {
      type: Sequelize.FLOAT,
      defaultValue: 0
    },
    kwh_max: {
      type: Sequelize.FLOAT,
      defaultValue: 0
    }
  }, {
    classMethods: {
      // TODO: unit test any errors that occur
      collectRecent: function (model, time, device_id) {
        var self = this;

        var rounded = roundTime(time, ONE_MINUTE);

        var def = bluebird.defer();
        var promise = def.promise;

        promise.success = function (fn) {
          return promise.then(fn);
        };

        promise.error = function (fn) {
          return promise.then(function () {}, fn);
        };

        model.findAll().success(function (consumptions) {
          //console.log(consumptions.map(function (con) { return con.values }));

          console.log(consumptions.map(function (con) {
            return con.values;
          }).filter(function (con) {
            return con.time > rounded && con.time <= time && con.device_id === device_id
          }))

          model.findAll({
            where: [
              'time > ? && time <= ? && device_id = ?',
              rounded,
              time,
              device_id
            ]
          }).success(function (consumptions) {
            //console.log(consumptions.map(function (con) { return con.values }))

            var kwh = 0;
            if (consumptions.length) {
              kwh = consumptions.map(function (consumption) {
                return consumption.values.kwh_difference
              }).reduce(function (prev, curr) {
                return prev + curr;
              });
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
                    roundTime(minuteData.values.time, ONE_MINUTE).getTime()
              ) {
                self.create({
                  time: roundTime(time, ONE_MINUTE),
                  device_id: device_id,
                  kwh_sum: kwh
                }).success(function (minuteData) {
                  def.resolve(minuteData);
                }).error(function (err) {
                  def.reject(err);
                });
                return
              }


              minuteData.values.kwh_sum = kwh
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
        }).error(function (err) {
          def.reject(err);
        });

        return promise;
      }
    }
  });

var EnergyConsumptions = module.exports.EnergyConsumptions =
  sequelize.define('energy_consumptions', {
    device_id: {
      type: Sequelize.INTEGER.UNSIGNED,
      validate: {
        notNull: true
      }
    },
    time: {
      type: Sequelize.DATE,
      validate: {
        notNull: true
      }
    },

    // Below, kw, kwh, kwh_difference all have a default value explicitly set.
    // We don't want MySQL to have them be set to NULL, in case no value was
    // specified.

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
  }, {
    define: {
      freezeTableName: true
    },
    hooks: {
      beforeValidate: function (consumption, callback) {
        var self = this;

        // Look for the most recent entry.
        this.find({ where: [ 'device_id = ?', consumption.values.device_id ], order: 'time DESC' }).success(function (prev) {
          if (prev) {
            // console.log('Found a set of previous data');
            // console.log('Queried device id: %d', consumption.values.device_id);
            // console.log(prev.values);

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
                consumption.values.kwh - prev.values.kwh_difference
          } else {
            console.log('Didn\'t find anything');
            consumption.values.kwh_difference = consumption.values.kwh
          }

          callback(null, consumption);
        }).error(callback);
      },
      afterCreate: function (consumption, callback) {
        OneMinuteEnergyConsumptions.collectRecent(
          this, 
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
  async.each(data, function (con, callback) {
    EnergyConsumptions.create(con).success(function (con) {
      callback(null, con);
    }).error(function (err) {
      if (err instanceof Error) {
        return callback(err);
      }
      callback(new ValidationErrors(err));
    });
  }, function (err, result) {
    if (err) {
      def.reject(err);
    }

    def.resolve(result);
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