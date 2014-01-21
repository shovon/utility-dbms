var express = require('express');
var Sequelize = require('sequelize');
var async = require('async');

var PORT = 3000;

var sequelize = new Sequelize('test', 'root', 'root', {
  host: '127.0.0.1'
});

var EnergyConsumptions = sequelize.define('EnergyConsumptions', {
  device_id: Sequelize.INTEGER.UNSIGNED,
  kw: Sequelize.FLOAT,
  kwh: Sequelize.FLOAT,
});

sequelize.sync().success(function () {
  var app = express();

  app.use(express.bodyParser());

  app.post('/energy-consumptions', function (req, res, next) {
    async.each(req.body, function (con, callback) {
      EnergyConsumptions.create(con).success(function () {
        callback(null);
      }).error(function (err) {
        callback(err);
      });
    }, function (err) {
      if (err) {
        return next(err);
      }

      res.send('Success');
    });
  });

  app.listen(PORT);
  console.log('Listening %d', PORT);
}).error(function (err) {
  throw err;
});
