const users = require('./users');
const bcrypt = require('bcrypt');
const redissessions = require('./redissessions');

const rs = redissessions.rs;
const rsapp = redissessions.rsapp;

module.exports.login = function (req, res, next) {
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
};
