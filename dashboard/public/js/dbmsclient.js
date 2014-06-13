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

      $.ajax({
        url: self.host + '/data/' + series,
        type: 'GET',
        data: opts
      }).done(function (data) {
        callback(null, data);
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
