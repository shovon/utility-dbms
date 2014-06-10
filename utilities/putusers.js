const prompt = require('sync-prompt').prompt;
const levelup = require('levelup');
const bcrypt = require('bcrypt');

const UNMATCHING_PASSWORD_MESSAGE = 'Root password does not match.';

const leveldb = levelup('./.db');

const rootpassword = prompt.hidden('Enter root password: ');

leveldb.get('users:root', function (err, value) {
  if (err) {
    console.error(err);
    process.exit(1);
    return;
  }
  if (!value) {
    console.error(UNMATCHING_PASSWORD_MESSAGE);
    process.exit(1);
    return;
  }

  var result = bcrypt.compareSync(rootpassword, value.hash);
  if (!result) {
    console.error(UNMATCHING_PASSWORD_MESSAGE);
    process.exit(1);
    return;
  }

  const username = prompt('Enter username: ');
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

  var role = '';

  while (/(r|w|a)/.test(role)) {
    role = prompt(
      'Would you like your user to have the role of a(n)\n\
\n\
1) reader\n\
2) writer\n\
3) admin\n\
\n\
Pick either options 1, 2, or 3: '
    )
  }

  const salt = bcrypt.genSaltSync();
  const hash = bcrypt.hashSync(password, salt);

  leveldb.put(['users',username].join(':'), { hash: hash, role: role })
});