const prompt = require('sync-prompt').prompt;
const bcrypt = require('bcrypt');
const Datastore = require('nedb');

const users = new Datastore({ filename: './.db/users', autoload: true });

console.log('You are about to create a new user.');
console.log();

const username = prompt('Enter username: ');
var password;
var passwordRepeat;

while (!password || password.length < 6 || password !== passwordRepeat) {
  password = prompt.hidden('Enter new password  : ');
  if (password.length < 6) {
    console.log('Password must be at least 6 characters long.');
    continue;
  }
  passwordRepeat = prompt.hidden('Enter password again : ');
  if (password !== passwordRepeat) {
    console.log('The passwords do not match.');
  }
}

var role = '';
const roleRegex = /(r|w|b)/;

while (!roleRegex.test(role)) {
  role = prompt(
    'Would you like your user to have the role of a\n\
\n\
r) reader\n\
w) writer\n\
b) both\n\
\n\
Pick either options r, w, or b: '
  )
  if (!roleRegex.test(role)) {
    console.log();
    console.log('You must pick either r, w, or b');
    console.log();
  }
}

const salt = bcrypt.genSaltSync();
const hash = bcrypt.hashSync(password, salt);

users.update(
  { username: username },
  { username: username, hash: hash, role: role },
  { upsert: true },
  function (err, result) {
    if (err) {
      console.error(err);
      process.exit(1);
      return;
    }
    console.log('User updated.');
  }
)
