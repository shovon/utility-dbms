const mysql = require('mysql');
const express = require('express');
const settings = require('./settings');
const lodash = require('lodash');
const async = require('async');
const util = require('util');
const _ = require('lodash');
const bodyParser = require('body-parser');
const path = require('path');
const RedisSessions = require('redis-sessions');
const fs = require('fs');
const mkdirp = require('mkdirp');
const Datastore = require('nedb');
const crypto = require('crypto');
const bcrypt = require('bcrypt');
const cors = require('cors');

const users = new Datastore({ filename: './.db/users', autoload: true });
const rs = new RedisSessions();
const rsapp = 'dbms';

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

const seriesMapping = [];
function getSeries(label, callback) {
  var series = seriesMapping[label];

  if (!series) {
    return mysqlConnection.query(
      'SELECT * FROM time_series WHERE label = ?',
      [label],
      function (err, result) {
        if (err) { return callback(err); }
        if (!result || !result.length) {
          return mysqlConnection.query(
            'INSERT INTO time_series (label) VALUES (?)',
            [label],
            function (err) {
              if (err) { return callback(err); }
              getSeries(label);
            }
          );
        }
        var series = result[0];
        seriesMapping[label] = series;
        callback(null, series);
      }
    );
  }

  setImmediate(function () {
    callback(null, device);
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
              mysqlConnection.query(
                'INSERT INTO devices (real_device_id, series_id) VALUES (?, ?)',
                [id, series.id],
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

function restrictRead(req, res, next) {
  if (!req.query.session) {
    return res.send(403, 'You are not allowed here.');
  }
  rs.get({
    app: rsapp,
    token: req.query.session
  }, function (err, resp) {
    if (err) { return next(err); }
    if (!resp) { return res.send(403, 'Session does not exist.'); }
    users.find({ username: resp.id }, function (err, docs) {
      if (err) { return next(err); }
      if (!docs.length) {
        return rs.killsoid({ app: rsapp, id: resp.id }, function (err, resp) {
          if (err) { return next(err); }
          next(new Error('User with username not found'));
        });
      }
      if (docs[0].role !== 'r' && docs[0].role !== 'b') {
        return res.send(403, 'User is not allowed to read from here.');
      }
      next();
    });
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

app.use(cors());
app.use(bodyParser.json());

app.get(
  '/data/:series',
  restrictRead,
  function (req, res, next) {

    // TODO: when no interval is supplied, don't apply any aggregate functions.

    // TODO: should the data be retrieved in the SQL style? That is, whatever
    //   'column' the user requests (that is, either of mean, sum, min, or max),
    //   that's the column name that is going to be returned? Or should it be
    //   just returned as 'value'?

    // The list of query parameters that will affect the above query are:
    //
    //     func. Optional. Can be either of mean, min, max sum. Defaults to
    //       mean, when omitted.
    //     interval. Optional. Can be either of x, xm, xh, xd, xw, xmo, xy,
    //       where x is a decimal-formatted integer. Defaults to just x, when
    //       omitted.
    //     devices. Optional. URL-encoded JSON string with the properties `ids`,
    //       and `exclude`. `ids` is an array of device IDs, and `exclude` is a
    //       boolean, to determine whether or not `ids` will exclude the list of
    //       specified devices. When omitted, it will be assumed that the client
    //       intends to have all devices in the series
    //     from. Optional. an ISO 8601 string
    //     to. Optional. an ISO 8601 string

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

    if (
      req.query.interval && !/^\d+((m|h|d|w|y)o?)?$/.test(req.query.interval)
    ) {
      return res.send(400, 'Interval query invalid.');
    }

    // First thing's first: get the aggregate function. We'll just default to
    // the mean for the sake of it. Now, bear in mind that the aggregate
    // function terms used by MySQL is different from what we are going to be
    // using. In this case, 'mean' implies MySQL's `AVG`, 'min' implies `MIN`,
    // 'max' implies `MAX`, and sum implies `SUM`. Those mappings are found in
    // the hash below.

    const mysqlFunctionMapping = {
      mean: 'AVG',
      min: 'MIN',
      max: 'MAX',
      sum: 'SUM'
    };

    const aggregateFunction = req.query.func || 'mean';

    if (!mysqlFunctionMapping[aggregateFunction]) {
      return res.send(400, 'Aggregate function not supported.');
    }

    // Next, get the interval based on the user-supplied interval value. This
    // one is multi-part.

    const amount =
      (
        req.query.interval && parseInt(req.query.interval.match(/^\d+/)[0])
      ) || 1;
    // We don't want to work around bad inputs issued by users. Just throw an
    // error.
    if (amount <= 0 || (amount|0) !== amount) {
      return res.send(
        400,
        'The interval amount should be an integer greater than 0.'
      );
    }

    const granularityMatch =
      (
        req.query.interval && req.query.interval.match(/(m|h|d|w|y)o?$/)
      ) || null;

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

    var timeWindow = '';
    if (req.query.from || req.query.to) {
      timeWindow += 'AND ';
      var values = [];
      if (req.query.from) {
        timeWindow += 'time > ? '
        values.push(new Date(req.query.from));
      }
      if (req.query.to) {
        if (req.query.from) {
          timeWindow += 'AND '
        }
        timeWindow += 'time < ? '
        values.push(new Date(req.query.to));
      }
      timeWindow = mysql.format(timeWindow, values);
    }

    const sql = mysql.format(
      util.format(
        'SELECT\n \
            %s(%s) AS %s,\n \
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(time) / ?) * ?) AS time\n \
          FROM (\n \
            SELECT SUM(%s) AS %s,\n \
            time,\n \
            device_id\n \
            FROM %s\n \
            WHERE\n \
              device_id IN (\n \
                SELECT devices.id FROM devices \
                  INNER JOIN time_series ON devices.series_id = time_series.id \
                  WHERE time_series.label = ? %s\n \
              )\n \
              %s \
            GROUP BY device_id, time\n \
            ORDER BY time DESC\n \
          )\n AS summed\n \
          GROUP BY FLOOR(UNIX_TIMESTAMP(time) / ?) * ? ORDER BY time DESC',
        mysqlFunctionMapping[aggregateFunction],
        granularity === 's' ? 'value' : aggregateFunction,
        aggregateFunction,
        granularity === 's' ? 'value' : aggregateFunction,
        granularity === 's' ? 'value' : aggregateFunction,
        tableName,
        whereDevicesQuery,
        timeWindow
      ),
      [ interval, interval, seriesName, interval, interval ]
    );

    mysqlConnection.query(sql, function (err, result) {
      if (err) { return next(err); }
      res.send(result);
    });
  }
);

// Gets a list of all series.
app.get(
  '/series',
  restrictRead,
  function (req, res, next) {
    mysqlConnection.query(
      'SELECT label FROM time_series',
      function (err, result) {
        console.log(result);
        if (err) { return next(err); }
        res.json(result.map(function (series) {
          return series.label
        }));
      }
    )
  }
);

// Gets a list of all devices.
app.get(
  '/devices',
  restrictRead,
  function (req, res, next) {
    mysqlConnection.query(
      'SELECT \
          devices.real_device_id as id, \
          time_series.label AS series, \
          devices.name AS name \
        FROM devices \
      INNER JOIN time_series ON (devices.series_id = time_series.id)',
      function (err, result) {
        if (err) { return next(err); }
        var grouped = _.groupBy(result, function (device) {
          return device.series;
        });
        for (var key in grouped) {
          grouped[key] = grouped[key].map(function (device) {
            return _.pick(device, 'id', 'name');
          });
        }
        res.json(grouped);
      }
    );
  }
);

// Get a list of all devices in specified series.
app.get(
  '/devices/:series',
  restrictRead,
  function (req, res, next) {
    mysqlConnection.query(
      'SELECT devices.real_device_id as id, devices.name FROM devices \
      INNER JOIN time_series ON (devices.series_id = time_series.id) \
      WHERE time_series.label = ?',
      [req.params.series],
      function (err, result) {
        if (err) { return next(err); }
        res.json(result);
      }
    )
  }
);

app.post(
  '/data',
  restrictWrite,
  function (req, res, next) {
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

            // Finally, insert the data point that the client provided, as well
            // as update the running total.
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
                    // Add the to-be inserted value to the running total, if
                    // there was a row found. Otherwise, just add to 0.
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
  }
);

// Allows clients to log in.
app.post('/login', function (req, res, next) {
  users.find({ username: req.body.username }, function (err, docs) {
    if (err) { return next(err); }
    const doc = docs[0];
    if (!doc) {
      return res.send(403, 'Username and password don\'t match');
    }
    bcrypt.compare(req.body.password, doc.hash, function (err, result) {
      if (err) { return next(err); }
      if (!result) { res.send(403, 'Username and password don\'t match.'); }

      const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;

      return rs.create(
        {
          app: rsapp,
          id: doc.username,
          ip: ip
        },
        function (err, resp) {
          if (err) { return next(err); }
          res.send(resp);
        }
      );
    });
  });
});

app.listen(settings.get('port') || 4406, function () {
  console.log('DBMS server listening on port %s', this.address().port);
});
