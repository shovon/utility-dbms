import mysql from 'mysql';
import express from 'express';
import * as settings from './settings';
import _ from 'lodash';
import bodyParser from 'body-parser';
import cors from 'cors';
import moment from 'moment';
import https from 'https';
import fs from 'fs';
import path from 'path';

const cert = fs.readFileSync(path.join(__dirname, 'settings', 'keys.cert'));
const key = fs.readFileSync(path.join(__dirname, 'settings', 'keys.key'));
const ca = fs.readFileSync(path.join(__dirname, 'settings', 'ca.cert'));

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

function formatTime(time) {
  return moment(time).format('YYYY-MM-DD HH:mm:ss');
}

function mysqlQuery(...params) {
  return new Promise((resolve, reject) => {
    const cb = (err, results) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(results);
    };
    mysqlConnection.query(...[...params, cb]);
  });
}

async function getSeries(label) {
  if (!label) {
    throw new Error('label not defined');
  }

  const selectQuery = 'SELECT * FROM time_series WHERE label = ?';
  const results = await mysqlQuery(selectQuery, [label]);
  if (results && results.length) { return results; }
  var timeCreated = new Date();
  await mysqlQuery(`
    INSERT INTO time_series (label, time_created, time_modified)
    VALUES (?, ?, ?)
    `,
    [label, formatTime(timeCreated), formatTime(timeCreated)]
  );
  return getSeries(label);
}

// Get the device given the device id and time series. If none exist, a new one
// will be created, and that newly inserted row will be returned.
async function getDevice(id, seriesLabel) {

  const result = await mysqlQuery(`
    SELECT devices.id AS id, time_series.label AS series FROM devices
    INNER JOIN time_series ON (devices.series_id = time_series.id)
    WHERE devices.real_device_id = ? AND time_series.label = ?;
  `, [id, seriesLabel]);

  if (result && result.length) {
    return result[0];
  }

  const series = await getSeries(seriesLabel);
  if (!series || !series.length) {
    throw new Error('Something went wrong');
  }
  const timeCreated = new Date();
  await mysqlQuery(`
    INSERT INTO devices ( \
      real_device_id, \
      series_id, \
      time_created, \
      time_modified \
    ) \
    VALUES (?, ?, ?, ?)
    `, [ id, series[0].id, formatTime(timeCreated), formatTime(timeCreated) ]
  );
  return getDevice(id, seriesLabel);

}

app.use(cors());
app.use(bodyParser.json());

app.post(
  '/data',
  (req, res, next) => {(async function () {
    // For each data point the object's body will look like:
    //
    //     {
    //       series: <string>
    //       device_id: <string>
    //       value: <number>
    //       time: <ISO 8601 date string>
    //     }

    const pending = req.body.data.map(async function (point) {
      const device = await getDevice(point.device_id, point.series);
      if (!device) {
        throw new Error('Something went wrong');
      }
      await mysqlQuery(`
        INSERT INTO data_points (device_id, value, time)
        VALUE (?, ?, ?);
        `,
        [ device.id, point.value, new Date(point.time) ]
      );
    });

    await Promise.all(pending);

    res.send('Success.');
  }()).catch(next); }
);

const port = settings.get('writer:port') || 4406;
https.createServer({
  key, cert, ca, requestCert: true, rejectUnauthorized: false
}, app).listen(port, function () {
  console.log('Writer server listening on port %s', this.address().port);
});

// app.listen(, function () {
//   console.log('Writer server listening on port %s', this.address().port);
// });
