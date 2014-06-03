const mysql = require('mysql');
const express = require('express');
const settings = require('./settings');
const lodash = require('lodash');
const async = require('async');
const util = require('util');
const _ = require('lodash');
const bodyParser = require('body-parser');

// TODO: all client errors should be responded using a 4xx error status code.
//   hence, avoid calling the `next` callback.

const mysqlSettings = _.pick(
  settings.get('mysql'),
  [ 'host', 'user', 'password', 'database' ]
);

const mysqlConnection = mysql.createConnection(mysqlSettings);
mysqlConnection.connect();

const app = express();

// The device ID in the database, and the device ID from the house can be
// entirely different. Hence there should be a physical device ID to database
// device ID mapping.
// TODO: variable-only caching is really bad. The devices should also be cached
//   in Redis.
const devicesMapping = [];
function getDevice(id, series, callback) {
  var device = devicesMapping[[id, series].join(':')];

  // This means that the device was never cached, and hence we have to query the
  // database.
  if (!device) {
    return mysqlConnection.query(
      'SELECT * FROM devices WHERE real_device_id = ? AND type = ?;',
      [id, series],
      function (err, result) {
        if (err) { return callback(err); }

        // This means that the device is not even in the database. Insert it,
        // and get the row from the database.
        if (!result || !result.length) {
          return mysqlConnection.query(
            'INSERT INTO devices (real_device_id, type) VALUES (?, ?)',
            [id, series],
            function (err) {
              if (err) { return callback(err); }
              getDevice(id, series, callback);
            }
          );
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

app.use(bodyParser.json());

app.get('/data/:series', function (req, res, next) {
  req.param.series.
  mysqlConnection.query(
    'SELECT * FROM data_points WHERE '
  )
  res.send(501, 'Coming soon.');
});

app.get('/series', function (req, res, next) {
  mysqlConnection.query(
    'SELECT DISTINCT(type) as type FROM devices',
    function (err, result) {
      if (err) { return next(err); }
      res.json(result.map(function (device) {
        return device.type
      }));
    }
  )
});

app.get('/devices', function (req, res, next) {
  mysqlConnection.query(
    'SELECT real_device_id as id, type, name FROM devices',
    function (err, result) {
      if (err) { return next(err); }
      var grouped = _.groupBy(result, function (device) {
        return device.type;
      });
      for (var key in grouped) {
        grouped[key] = grouped[key].map(function (device) {
          return _.pick(device, 'id', 'name');
        });
      }
      res.json(grouped);
    }
  );
});

app.get('/devices/:series', function (req, res, next) {
  mysqlConnection.query(
    'SELECT real_device_id as id, name FROM devices WHERE type = ?',
    [req.params.series],
    function (err, result) {
      if (err) { return next(err); }
      res.json(result);
    }
  )
});

app.post('/data', function (req, res, next) {
  // TODO: accept a more compact JSON format.

  // We should store the running total into the database, and expect that the
  // client never sends running total data. It should be upto the database
  // management system to compute the running total.

  // For each data point the object's body will look like:
  //
  //     {
  //       series: <string>
  //       device_id: <string>
  //       value: <number>
  //       time: <ISO 8601 date string>
  //     }

  // TODO: have another way to check if body has the correct elements in it.
  // TODO: ensure that all insertions occur in parallel.
  // TODO: check to ensure that new insertion requests have larger times.

  if (!_.isArray(req.body)) {
    return res.send(400, 'Data must be a JSON array.');
  }

  async.waterfall([

    // Store the actual data into the database.
    //
    // Remember, the `devices` parameter reperesents rows from the `devices`
    // table, with all columns loaded.
    function (callback) {

      async.each(req.body, function (item, callback) {

        async.waterfall([
          // This gets the device, and its corresponding ID in the database
          // table.
          function (callback) {
            getDevice(item.device_id, item.series, callback)
          },

          // Get the most recently inserted data point.
          function (device, callback) {
            mysqlConnection.query(
              'SELECT * FROM data_points WHERE device_id = ? \
              ORDER BY time DESC LIMIT 1',
              [device.id],
              function (err, result) {
                if (err) { return callback(err); }
                var row = result && result.length ? result[0] : null
                callback(null, device, row);
              }
            )
          },

          // Finally, insert the data point that the client provided, as well as
          // update the running total.
          function (device, row, callback) {
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
                  // Add the to-be inserted value to the running total, if there
                  // was a row found. Otherwise, just add to 0.
                  row ? row.running_total + item.value : item.value,
                  item.time
                ]
              );

            // Now do the actual insertion.
            mysqlConnection.query(insertionQuery,
              function (err, result) {
                // TODO: this shouldn't crash. Just gracefully move on.
                if (err) { return callback(err); }
                callback(null);
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

      async.each(req.body, function (item, callback) {

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
                [ device.id, roundedDownTime ],
                function (err, result) {
                  if (err) { return callback(err); }
                  callback(null, device, result);
                }
              );
            },

            // Insert/update aggregate data.
            function (device, result, callback) {
              // Since nothing came up, then it means we should insert a new
              // row.
              if (!result || !result.length) {
                return mysqlConnection.query(
                  'INSERT INTO ' + granularity.name + ' \
                  (device_id, mean, sum, min, max, time) \
                  VALUES (?, ?, ?, ?, ?, ?)',
                  [
                    device.id,
                    item.value,
                    item.value,
                    item.value,
                    item.value,
                    roundedDownTime
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
});

app.listen(settings.get('port'), function () {
  console.log('DBMS server listening on port %s', this.address().port);
});
