var express = require('express');
var models = require('./models');

var PORT = 3000;

function runServer() {
  var app = express();

  app.use(express.bodyParser());

  app.post('/energy-consumptions', function (req, res, next) {
    models.EnergyConsumptions.bulkCreate(req.body).success(function () {
      res.send('Success!');
    }).error(next);
  });

  app.use(function (err, req, res, next) {
    if (err instanceof models.ValidationErrors) {
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
