const prompt = require('sync-prompt').prompt;
const bcrypt = require('bcrypt');
const Datastore = require('nedb');
const async = require('async');

const users = new Datastore({ filename: '.db/users' });

var password;
var passwordRepeat;

while (!password || password.length < 6 || password !== passwordRepeat) {
  password = prompt.hidden('Enter root password  : ');
  if (password.length < 6) {
    console.log('Password must be at least 6 characters long.');
    continue;
  }
  passwordRepeat = prompt.hidden('Enter password again : ');
  if (password !== passwordRepeat) {
    console.log('The passwords do not match.');
  }
}

const salt = bcrypt.genSaltSync();
const hash = bcrypt.hashSync(password, salt);

users.find({ username: 'root' }, function (err, docs) {
  if (err) {
    console.error(err);
    process.exit(1);
    return;
  }
});