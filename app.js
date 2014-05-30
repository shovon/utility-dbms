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

app.post('/data', function (req, res) {
  // TODO: accept a more compact JSON format.

  // For each data point the object's body will look like:
  //
  //     {
  //       series: <string>
  //       device_id: <string>
  //       value: <number>
  //       running_total: <boolean>
  //       time: <ISO 8601 date string>
  //     }

  async.waterfall([

    // Store the energy consumer devices into the database.
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
      //     real_device_id = ? OR real_device_id = ? OR ...
      //       OR real_device_id = ?
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
            return device.real_device_id + ':' + device.type;
          });

          // Get all device IDs that are not in the database.
          var intersection = _.intersection(databaseDevices, devices);
          var toInsert = _.difference(devices, interesection);

          // Nothing to insert. Move on.
          if (!toInsert.length) {
            return setImmediate(finishCallback);
          }

          // Generate an insertion query for all devices that are not in the
          // database yet.
          var insertionQuery = toInsert.map(function(deviceID) {
            return mysql.format('INSERT INTO energy_consumer_devices (device_real_id) VALUES (?)', [deviceID]);
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
    function (devices, callback) {
      callback(new Error('Not yet implemented.'));
    },

    // Roll up the readings into buckets.
    function (callback) {

    }

  ], function (err) {
    if (err) { return next(err); }
    res.send('Success.');
  });
});
