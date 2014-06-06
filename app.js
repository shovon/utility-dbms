const mysql = require('mysql');
const express = require('express');
const settings = require('./settings');
const lodash = require('lodash');
const async = require('async');
const util = require('util');
const _ = require('lodash');
const bodyParser = require('body-parser');
const path = require('path');

// TODO: add password protection.

// TODO: all client errors should be responded using a 4xx error status code.
//   hence, avoid calling the `next` callback.

// TODO: consder having multiple MySQL users, each with their own permissions,
//   e.g. some can only read, while others can read and write.
//   
//   From the looks of it, however, there should absolutely be no need of any
//   users that are granted the ability to delete databases, drop tables, create
//   new tables, etc. Only write to tables, and read from tables.

// TODO: have the different series be their own tables.

// TODO: write migrations for what we have now.

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
// TODO: variable-only caching only lasts as long as the DBMS server is alive.
//   Perhaps store the mapping in a file, or a caching server such as Redis.
// TODO: perhaps unit/integration test this.
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
// TODO: this should really be its own web server.
app.use(express.static(path.join(__dirname, 'public')));

app.get('/data/:series', function (req, res, next) {

  // TODO: the readings should be the sum of the values of all the devices.

  // TODO: establish a limit as to how much data we are going to be retrieving.

  // TODO: when no interval is supplied, don't apply any aggregate functions.

  // TODO: have a `from` and `to` parameters. `From` will be the earliest in
  //   in time that the data was stored, and `to` will be latest.

  // This route will perform the following MySQL query:
  //
  //     SELECT
  //       <func>(<column associated with func>) AS
  //         <column associated with func>,
  //       FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(time) / <interval>) * <interval>)
  //         AS time
  //       FROM data_points<specific granularity, if any>
  //       <WHERE devices to query>
  //       GROUP BY FLOOR(UNIX_TIMESTAMP(time) / <interval>) * <interval>
  //
  // Where <WHERE devices to query> represents which devices should have its
  // values aggregated. When the client does not specify a list of devices to
  // query, then the WHERE clause will simply query all devices of the series
  // in question. Otherwise, it will query the specified device, or, if the
  // cient specified, filter out those devices.
  //
  // This is the case when the client did not specify any particular queries:
  //
  //     device_id IN (
  //       SELECT id FROM devices WHERE type = <series>
  //     )
  //
  // This is the case when the client specified which devices it wants queried:
  //
  //     device_id IN (
  //       SELECT id FROM devices WHERE type = <series> AND
  //         real_device_id IN (<list of devices>)
  //     )
  //
  // This is the case when the client specified which devices it wants filtered
  // out:
  //
  //     device_id IN (
  //       SELECT id FROM devices WHERE type = <series> AND
  //         real_device_id NOT IN (<list of devices>)
  //     )
  //
  // The list of query parameters that will affect the above query are:
  //
  //     func. Optional. Can be either of mean, min, max sum. Defaults to mean,
  //       when omitted.
  //     interval. Optional. Can be either of x, xm, xh, xd, xw, xmo, xy, where
  //       x is a decimal-formatted integer. Defaults to just x, when omitted.
  //     devices. Optional. URL-encoded JSON string with the properties `ids`,
  //       and `exclude`. `ids` is an array of device IDs, and `exclude` is a
  //       boolean, to determine whether or not `ids` will exclude the list of
  //       specified devices. When omitted, it will be assumed that the client
  //       intends to have all devices in the series
  //     from. Optional. To be implemented
  //     to. Optional. To be implemented

  const granularityIntervals = {
    s: 1,
    m: 60,
    h: 60 * 60,
    d: 60 * 60 * 24,
    w: 60 * 60 * 24 * 7,
    mo: 60 * 60 * 24 * 30,
    y: 60 * 60 * 24 * 365
  };

  // Before we start parsing, we need to validate all inputs.

  if (req.query.interval && !/^\d+(m|h|d|w|mo|y)?/.test(req.query.interval)) {
    return res.send(400, 'Interval query invalid.');
  }

  // First thing's first: get the aggregate function. We'll just default to the
  // mean for the sake of it. Now, bear in mind that the aggregate function
  // terms used by MySQL is different from what we are going to be using.
  // In this case, 'mean' implies MySQL's `AVG`, 'min' implies `MIN`, 'max'
  // implies `MAX`, and sum implies `SUM`. Those mappings are found in the hash
  // below.

  const mysqlFunctionMapping = {
    mean: 'AVG',
    min: 'MIN',
    max: 'MAX',
    sum: 'SUM'
  };

  const aggregateFunction = req.query.func || 'mean';

  // Next, get the interval based on the user-supplied interval value. This one
  // is multi-part.

  const amount =
    (req.query.interval && parseInt(req.query.interval.match(/^\d+/)[0])) || 1;
  // We don't want to work around bad inputs issued by users. Just throw an
  // error.
  if (amount <= 0 || (amount|0) !== amount) {
    return res.send(
      400,
      'The interval amount should be an integer greater than 0.'
    );
  }

  const granularityMatch =
    (req.query.interval && req.query.interval.match(/(m|h|d|w|mo|y)/)) || null;

  const granularity =
    (granularityMatch &&
    granularityMatch[0]) || 's';

  const interval = granularityIntervals[granularity] * amount;

  // Now, get the table name.

  const tableName =
    'data_points' + (granularity !== 's' ? '_1' + granularity : '');

  // Next, the per-device query.

  var perDevice;
  try {
    perDevice = req.query.devices ?
      JSON.parse(req.query.devices) : { all: true };
    if (!perDevice.all && !_.isArray(perDevice.ids)) {
      return res.send(400, 'The devices list is not valid.');
    }
  } catch (e) {
    return res.send(400, 'The devices list is not valid.')
  }

  // Afterwards, get the series name

  const seriesName = req.params.series;

  // Just the preliminary

  // Now, time to generate the SQL.

  var whereDevicesQuery;
  if (perDevice.all) {
    whereDevicesQuery = '';
  } else {
    var devicesList = mysql.format(perDevice.ids.map(function () {
      return '?'
    }).join(','), perDevice.ids);
    var andin = perDevice.exclude ? 'NOT IN' : 'IN';
    whereDevicesQuery =
      'AND real_device_id ' + andin + ' (' + devicesList + ')';
  }

  const sql = mysql.format(
    util.format(
      'SELECT \
            %s(%s) AS %s, \
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(time) / ?) * ?) AS time \
          FROM ( \
            SELECT SUM(%s) AS %s, \
            time, \
            device_id
            FROM %s
            WHERE \
              device_id IN ( \
                SELECT id FROM devices WHERE type = ? %s \
              ) \
            GROUP BY device_id
            ORDER BY time DESC
          )
          GROUP BY FLOOR(UNIX_TIMESTAMP(time) / ?) * ? ORDER BY time DESC',
      mysqlFunctionMapping[aggregateFunction],
      granularity === 's' ? 'value' : aggregateFunction,
      aggregateFunction,
      aggregateFunction,
      aggregateFunction,
      tableName,
      whereDevicesQuery
    ),
    [ interval, interval, seriesName, interval, interval ]
  );

  mysqlConnection.query(sql, function (err, result) {
    if (err) { return next(err); }
    res.send(result);
  });
});

// Gets a list of all series.
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

// Gets a list of all devices.
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

// Get a list of all devices in specified series.
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
