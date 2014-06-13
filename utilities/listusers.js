const Datastore = require('nedb');

const users = new Datastore({ filename: './.db/users', autoload: true });

const roles = { r: 'reader', w: 'writer', b: 'both' };

users.find({}, function (err, docs) {
  console.log();
  docs.forEach(function (doc) {
    console.log('username: %s', doc.username);
    console.log('role    : %s', roles[doc.role])
    console.log();
  });
});
