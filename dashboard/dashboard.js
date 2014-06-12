const express = require('express');
const path = require('path');
const settings = require('../settings');

const app = express();

app.use(express.static(path.join(__dirname, 'public')));
app.engine('jade', require('jade').__express);

app.get('/', function () {

});

app.listen(settings.get('port') || 4407, function () {
  console.log('Server listening on port %s', this.address().port);
});