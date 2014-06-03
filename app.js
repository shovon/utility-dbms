var mysql = require('mysql');
var express = require('express');
var settings = require('./settings');
var lodash = require('lodash');
var async = require('async');
var util = require('util');
var _ = require('lodash');
var bodyParser = require('body-parser');

// TODO: convert all constants to `const` type variables.

var mysqlSettings = _.pick(
  settings.get('mysql'),
  [ 'host', 'user', 'password', 'database' ]
);

var multistatementsMysqlSettings = _.assign(
  {}, mysqlSettings, { multipleStatements: true }
);

var singlestatementConnection = mysql.createConnection(mysqlSettings);
var multistatementConnection = mysql.createConnection(
  multistatementsMysqlSettings
);

singlestatementConnection.connect();
multistatementConnection.connect();

var app = express();

// The device ID in the database, and the device ID from the house can be
// entirely different. Hence there should be a physical device ID to database
// device ID mapping.
// TODO: variable caching is really bad. The devices should also be cached in
//   redis.
var devicesMapping = [];
function getDevice(id, series, callback) {
  var device = devicesMapping[[id, series].join(':')];

  // This means that the device was never cached, and hence we have to query the
  // database.
  if (!device) {
    return singlestatementConnection.query(
      'SELECT * FROM devices WHERE real_device_id = ? AND type = ?;',
      [id, series],
      function (err, result) {
        if (err) { return callback(err); }

        // This means that the device is not even in the database. Insert it,
        // and get the row from the database.
        if (!result || !result.length) {
          // TODO: instead of returning null, just insert the device into the
          //   database.
          return singlestatementConnection.query(
            // TODO: check to see whether the insertion and retrieval can be
            //   done in a single statement.
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
  res.send(501, 'Coming soon.');
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
            singlestatementConnection.query(
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
            var insertionQuery =
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
                  // Although, we won't be inserting timestamps, however, it's
                  // much easier to have MySQL convert the Unix timestamp, right
                  // before inserting the data. Maybe I will do a more
                  // appropriate insertion in the future.
                  new Date(new Date(item.time).getTime() / 1000)
                ]
              );

            // Now do the actual insertion.
            singlestatementConnection.query(insertionQuery,
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
      async.each(req.body, function (item, callback) {
        // Waterfall through the different granularities.
        async.waterfall([
          // The one minute granularity
          function (callback) {
            async.waterfall([
              function (callback) {
                // Get the device that is associated with the item in question.
                getDevice(item.device_id, item.series, callback);
              },

              // This is where the insertion/update happens.
              function (device, callback) {
                // Get a time that is rounded to the nearest minute.
                var coefficient = 1000 * 60;
                var roundedDownTime =
                  new Date(
                    Math.floor(
                      new Date(item.time).getTime() / coefficient
                    ) * coefficient
                  );

                // Query the most recent per-minute entry.
                singlestatementConnection.query(
                  'SELECT * FROM data_points_1m WHERE device_id = ? AND \
                  time >= ? ORDER BY time DESC LIMIT 1',
                  [ device.id, roundedDownTime ],
                  function (err, result) {
                    if (err) { return callback(err); }

                    // Since nothing came up, then just insert a new row.
                    if (!result || !result.length) {
                      return singlestatementConnection.query(
                        'INSERT INTO data_points_1m \
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
                      );
                    }

                    // Something came up. So let's run some computation.
                    var row = result[0];
                    var mean = (row.mean + item.value) / 2;
                    var sum = row.sum + item.value;
                    var min = item.value < row.min ? item.value : row.min;
                    var max = item.value > row.max ? item.value : row.max;

                    singlestatementConnection.query(
                      'UPDATE data_points_1m \
                      SET mean = ?, sum = ?, min = ?, max = ? \
                      WHERE id = ?',
                      [ mean, sum, min, max, row.id ],
                      function (err, result) {
                        if (err) { return callback(err); }
                        callback(null);
                      }
                    );
                  }
                );
              }
            ], callback);
          }
        ], callback);
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
