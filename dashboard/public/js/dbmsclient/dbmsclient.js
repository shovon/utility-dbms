;(function () {
  function login(username, password, host, callback) {
    $.ajax({
      url: host + '/login',
      contentType: 'application/json',
      data: JSON.stringify({
        username: username,
        password: password
      }),
      method: 'POST'
    }).done(function (body) {
      callback(null, body.token);
    });
  }

  window.DBMSClient = DBMSClient;
  function DBMSClient(username, password, host) {
    this.username = username;
    this.password = password;
    this.host = host;
    this.session = null;
  }

  DBMSClient.prototype.login = function (callback) {
    var self = this;
    login(this.username, this.password, this.host, function (err, token) {
      self.session = token;
      callback(err);
    });
  };

  // TODO: there are a lot of repetitions going on, regarding log-in.

  function toDate(str, date) {
    date = date || new Date();
    date = new Date(date);

    var datestrings = str.split(/\s+/);
    str = datestrings.shift();

    var number = str.match(/\d+/)[0];
    var unit = str.match(/(s|m|h|d|w|y)o?/)[0];

    switch (unit) {
      case 's':
        var seconds = date.getSeconds();
        seconds -= number;
        date.setSeconds(seconds);
        break;
      case 'm':
        var minutes = date.getMinutes();
        minutes -= number;
        date.setMinutes(minutes);
        break;
      case 'h':
        var hours = date.getHours();
        hours -= number;
        date.setHours(hours);
        break;
      case 'd':
        var days = date.getDate();
        days -= number;
        date.setDate(days);
        break;
      case 'w':
        var days = date.getDate();
        days -= number * 7;
        date.setDate(days);
        break;
      case 'mo':
        var months = date.getMonth();
        months -= number;
        date.setMonth(months);
        break;
      case 'y':
        var years = date.getFullYear();
        years -= number;
        date.setFullYear(years);
        break;
      default:
        return new Date('');
    }

    if (datestrings.length) {
      return toDate(datestrings.join(' '), date);
    }

    return date;
  }

  DBMSClient.prototype.getData = function (series, options, callback) {
    options = options || {};
    var self = this;
    if (!this.session) {
      return this.login(function (err) {
        if (err) { return callback(err); }
        getData(callback);
      });
    }
    getData(callback);
    function getData(callback) {
      var opts = {};

      for (var key in options) {
        opts[key] = options[key];
      }

      opts.session = self.session;

      function isShortcodeDate(str) {
        return /^\d+((m|h|d|w|y)o?)?/.test(str);
      }

      if (opts.from) {
        if (isShortcodeDate(opts.from)) {
          opts.from = toDate(opts.from);
        } else {
          opts.from = new Date(opts.from);
        }
      }

      if (opts.to) {
        if (isShortcodeDate(opts.to)) {
          opts.to = toDate(opts.to);
        } else {
          opts.to = new Date(opts.to);
        }
      }

      if (opts.devices) {
        opts.devices = JSON.stringify(opts.devices);
      }

      var finalOpts = {};

      var props =
        [ 'func', 'interval', 'devices', 'from', 'to', 'groupbyhour' ];
      props.forEach(function (prop) {
        opts[prop] && (finalOpts[prop] = opts[prop]);
      });

      $.ajax({
        url: self.host + '/data/' + series,
        type: 'GET',
        data: opts
      }).done(function (data) {
        callback(null, data.map(function (point) {
          if (typeof point.hour == 'number') {
            return {
              hour: point.hour,
              value: point.value
            };
          }
          return {
            time: new Date(point.time),
            value: point.value
          };
        }));
      }).fail(function (xhr, status) {
        console.log(xhr);
      });
    }
  };

  DBMSClient.prototype.getSeries = function (callback) {
    var self = this;
    if (!this.session) {
      return this.login(function (err) {
        if (err) { return callback(err); }
        getSeries(callback);
      });
    }
    getSeries(callback);
    function getSeries(callback) {
      $.ajax({
        url: self.host + '/series',
        type: 'GET',
        data: {
          session: self.session
        }
      }).done(function (data) {
        callback(null, data);
      }).fail(function (xhr, status) {
        console.log(xhr);
      })
    }
  };

  DBMSClient.prototype.getDevicesForSeries = function (series, callback) {
    var self = this;
    if (!this.session) {
      return this.login(function (err) {
        if (err) { return callback(err); }
        getDevicesForSeries(callback);
      });
    }
    getDevicesForSeries(callback);
    function getDevicesForSeries(callback) {
      $.ajax({
        url: self.host + '/devices/' + series,
        type: 'GET',
        data: {
          session: self.session
        }
      }).done(function (data) {
        callback(null, data);
      }).fail(function (xhr, status) {
        console.log(xhr);
      });
    }
  };
}());
