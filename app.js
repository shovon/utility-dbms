var mysql = require('mysql');
var express = require('express');
var settings = require('./settings');
var lodash = require('lodash');
var async = require('async');
var util = require('util');
var _ = require('lodash');
var bodyParser = require('body-parser');

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

  async.waterfall([

    // Store the data point devices into the database.
    function (callback) {
      // TODO: cache the devices query so that we don't end up having to query
      //   the database direclty every time when we want to see wether or not
      //   any given device exists in the database.

      // Extract the consumption value's device ID only.
      var devices = req.body.map(function (point) {
        // Do note that `point.device_id` is the ID that is identified in the
        // physical device itself. The device ID that will be stored in the
        // database is an index to a particular row.
        return [ point.device_id, point.series ];
      });

      // The WHERE statement.
      //
      // It should generate a string that looks like
      //
      //     (real_device_id = ? AND type = ?) OR
      //       (real_device_id = ? AND type = ?) OR ... OR 
      //       (real_device_id = ? AND type = ?)
      //
      // for N devices.
      var where = devices
        .map(function (device) {
          return mysql.format('(real_device_id = ? AND type = ?)', device)
        })
        .join(' OR ');

      // The query.
      var query = util
        .format(
          'SELECT * FROM devices WHERE %s',
          where
        );

      // Query the devices.
      singlestatementConnection.query(
        query,
        function (err, result) {
          if (err) { return callback(err); }

          // Instead of calling `callback`, call this function. Keeps the
          // `callback` call DRY.
          function finishCallback() {
            callback(null, result);
          }

          // Extract the device's consumption value's device ID only.
          var databaseDevices = result.map(function (device) {
            return JSON.stringify({
              real_device_id: device.real_device_id,
              type: device.type
            });
          });

          var devicesTupleString = devices.map(function (device) {
            return JSON.stringify({
              real_device_id: device[0],
              type: device[1]
            });
          });

          // Get all device IDs that are not in the database.
          var intersection = _.intersection(databaseDevices, devicesTupleString);
          var toInsert = _.difference(devicesTupleString, intersection);

          // Nothing to insert. Move on.
          if (!toInsert.length) {
            return setImmediate(finishCallback);
          }

          // Generate an insertion query for all devices that are not in the
          // database yet.
          // TODO: the query can be completed in a single statement.
          var insertionQuery = toInsert.map(function(device) {
            device = JSON.parse(device);
            var arr = [
              device.real_device_id,
              device.type
            ]
            return mysql.format(
              'INSERT INTO devices (real_device_id, type) VALUES (?, ?)', arr
            );
          }).join(';');

          // Now perform the insertion.
          multistatementConnection.query(insertionQuery, function (err, result) {
            if (err) { return callback(err); }
            finishCallback();
          });
        }
      )
    },

    // Store the actual data into the database.
    //
    // Remember, the `devices` parameter reperesents rows from the `devices`
    // table, with all columns loaded.
    function (devices, callback) {
      // This is where we will be storing the devices retrieved from the
      // database. They will be indexed by the string
      //
      //     <real device ID>:<series name>
      //
      // This exists so that we can get a particular device row's ID column
      // value more quickly.
      var devicesHash = {};
      devices.forEach(function (device) {
        devicesHash[device.real_device_id + ':' + device.type] = device;
      });

      async.each(req.body, function (item, callback) {
        databaseDeviceId = devicesHash[item.device_id + ':' + item.series].id

        // Start out by getting the most recently inserted data point.
        // TODO: so far, we are issuing a query for every device. It shouldn't
        //   many queryes, but just one query for all the devices that are
        //   having their values inserted. Of course, for the entire list of
        //   devices, there will undoubtedly be more than one queries
        //   (hopefully at most two), but it's **not** going to be per device.
        singlestatementConnection.query(
          'SELECT * FROM data_points WHERE device_id = ? \
          ORDER BY time DESC LIMIT 1',
          [databaseDeviceId],
          function (result, err) {
            // TODO: this shouldn't crash. Just gracefully move on.
            if (err) { return callback(err); }

            // Extract the row, if one exists. If it doesn't exist, then the row
            // is nil.
            var row = result && result.lenth ? result[0] : null;

            // Now do the actual insertion.
            singleStatementConnection.query(
              'INSERT INTO data_points (device_id, value, running_total, time) \
              VALUES (?, ?, ?, FROM_UNIXTIME(?))'
              [
                databaseDeviceId,
                item.value,
                // Add the to-be inserted value to the running total, if there
                // was a row found. Otherwise, just add to 0.
                row ? row.running_total + item.value : item.value,
                // Although, we won't be inserting timestamps, however, it's
                // much easier to have MySQL convert the Unix timestamp, right
                // before inserting the data. Maybe I will do a more appropriate
                // insertion in the future.
                new Date(item.time).getTime() / 1000
              ],
              function (result, err) {
                // TODO: this shouldn't crash. Just gracefully move on.
                if (err) { return callback(err); }
                callback(null);
              }
            )
          }
        );
      }, function (err) {
        if (err) { return callback(err); }
        callback(devices);
      });
    },

    // Roll up the readings into buckets.
    function (devices, callback) {
      callback(new Error('Not yet implemented.'));
    }

  ], function (err) {
    if (err) { return next(err); }
    res.send('Success.');
  });
});

app.listen(settings.get('port'), function () {
  console.log('DBMS server listening on port %s', this.address().port);
});
