const mysql = require('mysql');
const express = require('express');
const settings = require('./settings');
const lodash = require('lodash');
const async = require('async');
const util = require('util');
const _ = require('lodash');
const bodyParser = require('body-parser');
const path = require('path');
const redissessions = require('./redissessions');
const fs = require('fs');
const mkdirp = require('mkdirp');
const users = require('./users');
const crypto = require('crypto');
const bcrypt = require('bcrypt');
const cors = require('cors');
const routes = require('./routes');
const moment = require('moment');

const rs = redissessions.rs;
const rsapp = redissessions.rsapp;

// TODO: all client errors should be responded using a 4xx error status code.
//   hence, avoid calling the `next` callback.

// TODO: consder having multiple MySQL users, each with their own permissions,
//   e.g. some can only read, while others can read and write.
//   
//   From the looks of it, however, there should absolutely be no need of any
//   users that are granted the ability to delete databases, drop tables, create
//   new tables, etc. Only write to tables, and read from tables.

// TODO: all error responses should be in the same format and MIME type as what
//   the user has initially requested.

const mysqlSettings = _.pick(
  settings.get('mysql'),
  [ 'host', 'user', 'password', 'database' ]
);

const mysqlConnection = mysql.createConnection(mysqlSettings);
mysqlConnection.connect();

const app = express();

const seriesMapping = {};
const seriesReadQueue = [];
var busy = false;

function formatTime(time) {
  return moment(time).format('YYYY-MM-DD HH:mm:ss');
}

function getSeries(label, callback) {
  // TODO: refactor the code so that there aren't any repetitions.

  var series = seriesMapping[label];

  function ret(err, series) {
    callback(err, series);
    busy = false;
    if (seriesReadQueue.length) {
      var request = seriesReadQueue.pop();
      setImmediate(function () {
        getSeries(request[0], request[1]);
      });
    }
  }

  if (!series) {
    if (busy) {
      seriesReadQueue.unshift(Array.prototype.slice.call(arguments));
      return;
    }
    busy = true;
    var selectQuery = 'SELECT * FROM time_series WHERE label = ?';
    return mysqlConnection.query(
      selectQuery,
      [label],
      function (err, result) {
        if (err) { return callback(err); }
        if (!result || !result.length) {
          var timeCreated = new Date();
          return mysqlConnection.query(
            'INSERT INTO time_series ( \
              label, \
              time_created, \
              time_modified \
            ) VALUES (?, ?, ?)',
            [label, formatTime(timeCreated), formatTime(timeCreated)],
            function (err) {
              if (err) { return ret(err); }

              // No choice but to repeat code here, unfortunately.
              mysqlConnection.query(
                selectQuery,
                [label],
                function (err, result) {
                  if (err) { return callback(err); }
                  if (!result || !result.length) {
                    return callback(new Error('Can\'t create series'));
                  }
                  var series = result[0];
                  seriesMapping[label] = series;
                  ret(null, series);
                }
              )
            }
          );
        }
        var series = result[0];
        seriesMapping[label] = series;
        ret(null, series);
      }
    );
  }

  setImmediate(function () {
    ret(null, series);
  });
}

// The device ID in the database, and the device ID from the house can be
// entirely different. Hence there should be a physical device ID to database
// device ID mapping.
// TODO: perhaps unit/integration test this.
const devicesMapping = [];
function getDevice(id, series, callback) {
  var device = devicesMapping[[id, series].join(':')];

  if (!device) {
      // This means that the device was never cached, and hence we have to query
      // the database.

    return mysqlConnection.query(
      // This query should return a table with the following columns:
      //
      // - id, that represents the database ID.
      // - series, that represents the time series label.
      'SELECT devices.id AS id, time_series.label AS series FROM devices \
      INNER JOIN time_series ON (devices.series_id = time_series.id) \
      WHERE devices.real_device_id = ? AND time_series.label = ?;',
      [id, series],
      function (err, result) {
        if (err) { return callback(err); }

        // This means that the device is not even in the database. Insert it,
        // and get the row from the database.
        if (!result || !result.length) {
          return async.waterfall([
            // First, get the row ID of the requested series.
            function (callback) {
              getSeries(series, callback);
            },
            // Next, use the received series ID and store it in the row along
            // with the new device.
            function (series, callback) {
              const timeCreated = new Date();
              mysqlConnection.query(
                'INSERT INTO devices ( \
                  real_device_id, \
                  series_id, \
                  time_created, \
                  time_modified \
                ) \
                VALUES (?, ?, ?, ?)',
                [
                  id,
                  series.id,
                  formatTime(timeCreated),
                  formatTime(timeCreated)
                ],
                function (err) {
                  if (err) { return callback(err); }
                  callback(null);
                }
              );
            }
          ], function (err) {
            if (err) { return callback(err); }
            getDevice(id, series, callback);
          });
        }

        var device = result[0];
        devicesMapping[[id, series].join(':')] = device;
        callback(null, device);
      }
    );
  }

  setImmediate(function () {
    callback(null, device);
  });
}

function restrictWrite(req, res, next) {
  if (!req.body.session) {
    return res.send(403, 'You are not allowed here.');
  }
  rs.get({
    app: rsapp,
    token: req.body.session
  }, function (err, resp) {
    if (err) { return next(err); }
    if (!resp) { return res.send(403, 'Session does not exist.'); }
    users.find({ username: resp.id }, function (err, docs) {
      if (err) { return next(err); }
      if (!docs.length) {
        return rs.killsoid({ app: rsapp, id: resp.id }, function (err, resp) {
          if (err) { return next(err); }
          next(new Error('User with username not found'));
        });
      }
      if (docs[0].role !== 'w' && docs[0].role !== 'b') {
        return res.send(403, 'User is not allowed to write to here.');
      }
      next();
    })
  });
}

// TODO: maybe this should be disabled.
app.use(cors());

app.use(bodyParser.json());

app.post(
  '/data',
  restrictWrite,
  function (req, res, next) {
    // TODO: accept a more compact JSON format.
    // TODO: optimize this. Too many queries, which is causing major slow-downs.

    // For each data point the object's body will look like:
    //
    //     {
    //       series: <string>
    //       device_id: <string>
    //       value: <number>
    //       time: <ISO 8601 date string>
    //     }

    // TODO: have another way to check if body has the correct elements in it.
    // TODO: check to ensure that new insertion requests have larger times.

    if (!_.isArray(req.body.data)) {
      return res.send(400, 'Data must be a JSON array.');
    }

    async.waterfall([

      // Store the actual data into the database.
      //
      // Remember, the `devices` parameter reperesents rows from the `devices`
      // table, with all columns loaded.
      function (callback) {

        async.each(req.body.data, function (item, callback) {

          async.waterfall([
            // This gets the device, and its corresponding ID in the database
            // table.
            function (callback) {
              getDevice(item.device_id, item.series, callback)
            },

            // Get the most recently inserted data point. This is to compute the
            // running total.
            function (device, callback) {
              mysqlConnection.query(
                'SELECT * FROM running_total_cache WHERE device_id = ? limit 1',
                [device.id],
                function (err, result) {
                  if (err) { return callback(err); }
                  var row = result && result.length ? result[0] : null;
                  callback(null, device, row);
                }
              )

              // mysqlConnection.query(
              //   'SELECT * FROM data_points WHERE device_id = ? \
              //   ORDER BY time DESC LIMIT 1',
              //   [device.id],
              //   function (err, result) {
              //     if (err) { return callback(err); }
              //     var row = result && result.length ? result[0] : null
              //     callback(null, device, row);
              //   }
              // )
            },

            // Finally, insert the data point that the client provided, as well
            // as update the running total.
            function (device, row, callback) {
              // N.B.: `row` is a row from the `running_total_cache` table.

              const insertionQuery =
                mysql.format(
                  'INSERT INTO data_points ( \
                    device_id, \
                    value, \
                    running_total, \
                    time \
                  ) VALUES (?, ?, ?, ?)',
                  [
                    device.id,
                    item.value,
                    // Add the to-be inserted value to the running total, if
                    // there was a row found. Otherwise, just add to 0.
                    row ? row.running_total + item.value : item.value,
                    formatTime(new Date(item.time))
                  ]
                );

              // Now do the actual insertion.
              mysqlConnection.query(insertionQuery,
                function (err, result) {
                  if (err) { return callback(err); }
                  callback(null, device, row);
                }
              );
            },

            // Update the running totals cache.
            function (device, row, callback) {
              var sql = '';
              if (!row) {
                sql = mysql.format(
                  'INSERT INTO running_total_cache ( \
                    device_id, \
                    running_total \
                  ) VALUES (?, ?)',
                  [
                    device.id,
                    item.value
                  ]
                );
              } else {
                sql = mysql.format(
                  'UPDATE running_total_cache SET running_total = ? WHERE device_id = ?',
                  [
                    item.value + row.running_total,
                    device.id
                  ]
                );
                console.log(item.value + row.running_total);
              }

              mysqlConnection.query(
                sql,
                function (err, result) {
                  if (err) { return callback(err); }
                  callback(null, device, row);
                }
              );
            }
          ], function (err) {
            if (err) { return callback(err); }
            callback(null);
          })
        }, function (err) {
          if (err) { return callback(err); }
          callback(null);
        });
      },

      // Roll up the readings into buckets.
      function (callback) {
        // Iterate through each items and insert/update rows for the different
        // granularities.

        const granularities = [
          { name: 'data_points_1m', interval: 1000 * 60 },
          { name: 'data_points_1h', interval: 1000 * 60 * 60 },
          { name: 'data_points_1d', interval: 1000 * 60 * 60 * 24 },
          { name: 'data_points_1w', interval: 1000 * 60 * 60 * 24 * 7 },
          { name: 'data_points_1mo', interval: 1000 * 60 * 60 * 24 * 30 },
          { name: 'data_points_1y', interval: 1000 * 60 * 60 * 24 * 365 }
        ];

        async.each(req.body.data, function (item, callback) {

          // Waterfall through the different granularities.
          async.each(granularities, function (granularity, callback) {
            // Get the time that is rounded to the nearest minute.
            const roundedDownTime =
              new Date(
                Math.floor(
                  new Date(item.time).getTime() / granularity.interval
                ) * granularity.interval
              );
            async.waterfall([
              function (callback) {
                // Just get the device associated with the device ID and the
                // series name.
                getDevice(item.device_id, item.series, callback);
              },

              // Query for the most recent entry.
              function (device, callback) {
                mysqlConnection.query(
                  'SELECT * FROM ' + granularity.name + ' \
                  WHERE device_id = ? AND time >= ? ORDER BY time DESC LIMIT 1',
                  [ device.id, formatTime(roundedDownTime) ],
                  function (err, result) {
                    if (err) { return callback(err); }
                    callback(null, device, result);
                  }
                );
              },

              // Get the running total associated with the device ID.
              function (device, result, callback) {
                mysqlConnection.query(
                  'SELECT * FROM running_total_cache WHERE device_id = ?',
                  [ device.id ],
                  function (err, runningTotal) {
                    if (err) { return callback(err); }
                    if (!runningTotal.length) {
                      return callback(
                        new Error(
                          'There should have been data in the ' +
                          '`running_total_cache`'
                        )
                      );
                    }
                    callback(null, device, result, runningTotal[0]);
                  }
                );
              },

              // Insert/update aggregate data.
              function (device, result, runningTotalRow, callback) {
                // Since nothing came up, then it means we should insert a new
                // row.
                if (!result || !result.length) {
                  return mysqlConnection.query(
                    'INSERT INTO ' + granularity.name + ' \
                    (device_id, mean, sum, min, max, time, running_total) \
                    VALUES (?, ?, ?, ?, ?, ?, ?)',
                    [
                      device.id,
                      item.value,
                      item.value,
                      item.value,
                      item.value,
                      formatTime(roundedDownTime),
                      runningTotalRow.running_total
                    ],
                    function (err, result) {
                      if (err) { return callback(err); }
                      callback(null);
                    }
                  )
                }

                // Something came up. We should then run some computation.

                const row = result[0];
                const mean = (row.mean + item.value) / 2;
                const sum = row.sum + item.value;
                const min = item.value < row.min ? item.value : row.min;
                const max = item.value > row.max ? item.value : row.max;

                mysqlConnection.query(
                  'UPDATE ' + granularity.name + ' \
                  SET mean = ?, sum = ?, min = ?, max = ? \
                  WHERE id = ?',
                  [ mean, sum, min, max, row.id ],
                  function (err, result) {
                    if (err) { return callback(err); }
                    callback(null);
                  }
                )
              }
            ], callback);
          }, callback);
        }, callback);
      }

    ], function (err) {
      if (err) { return next(err); }
      res.send('Success.');
    });
  }
);

// Allows clients to log in.
app.post('/login', routes.login);

app.listen(settings.get('writer:port') || 4406, function () {
  console.log('Writer server listening on port %s', this.address().port);
});
