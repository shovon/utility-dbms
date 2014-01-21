var express = require('express');
var models = require('./models');
var async = require('async');

var PORT = 3000;

function ValidationErrors(err) {
  var finalMessage = [];
  Object.keys(err).forEach(function (key) {
    finalMessage.push(key + ': ' + err[key]);
  });
  this.message = finalMessage.join('\n');
}

ValidationErrors.prototype = Error.prototype;

function runServer() {
  var app = express();

  app.use(express.bodyParser());

  app.post('/energy-consumptions', function (req, res, next) {
    async.each(req.body, function (con, callback) {
      models.EnergyConsumptions.create(con).success(function () {
        callback(null);
      }).error(function (err) {
        callback(new ValidationErrors(err));
      });
    }, function (err) {
      if (err) {
        return next(err);
      }

      res.send('Success');
    });
  });

  app.use(function (err, req, res, next) {
    if (err instanceof ValidationErrors) {
      return res.send(400, err.message);
    }
    return next(err);
  });


  app.listen(PORT);
  console.log('Listening %d', PORT);
}

models.sequelize.sync({ force: true }).success(runServer).error(function (err) {
  throw err;
});
