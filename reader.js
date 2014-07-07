const express = require('express');
const settings = require('./settings');
const cors = require('cors');

const app = express();

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

app.use(cors());

app.get(
  '/data/:series',
  restrictRead,
  function (req, res, next) {

    // TODO: when no interval is supplied, don't apply any aggregate functions.

    // TODO: be able to handle shortcodes, just like what the dashboard's
    //   `dbmsclient.js` does.

    // The list of query parameters that will affect the SQL query are:
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
    //     groupbyhour. Optional. Groups the data further by hours. The value is
    //       any of the following aggregate functions: mean, min, max, sum.

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

    var granularity =
      (granularityMatch &&
      granularityMatch[0]) || 's';

    var interval = granularityIntervals[granularity] * amount;

    const groupbyhour = req.query.groupbyhour;
    if (
      groupbyhour && !mysqlFunctionMapping[aggregateFunction]
    ) {
      return res.send(400, 'Aggregate function not supported.');
    } else if (groupbyhour) {
      granularity = 'h';
      interval = granularityIntervals[granularity];
    }

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

    const aggregateSQL = mysql.format(
      util.format(
        'SELECT\n \
            %s(value) AS value,\n \
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(time) / ?) * ?) AS time\n \
          FROM (\n \
            SELECT SUM(%s) AS value,\n \
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
        tableName,
        whereDevicesQuery,
        timeWindow
      ),
      [ interval, interval, seriesName, interval, interval ]
    );

    var sql = aggregateSQL;
    if (groupbyhour) {
      sql = util.format(
        'SELECT\n \
            %s(value) AS value,\n \
            HOUR(time) AS hour\n \
          FROM (\n \
            %s\n \
          ) AS hourly\n \
          GROUP BY HOUR(time) ORDER BY HOUR(time) DESC',
        mysqlFunctionMapping[groupbyhour],
        aggregateSQL
      );
    }

    var start = new Date();
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
    );
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

app.listen(settings.get('reader:port') || 4407, function () {
  console.log('DBMS server listening on port %s', this.address().port);
});
