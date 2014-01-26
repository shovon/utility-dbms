var expect = require('expect.js');
var roundTime = require('./models').roundTime;

describe('unit tests', function () {
  describe('roundTime', function () {
    it('should floor to the nearest 10 seconds', function () {
      var date = new Date(0);
      date.setHours(12);
      date.setMinutes(32);
      date.setSeconds(54);
      date.setMilliseconds(40);

      var expected = new Date(0);
      expected.setHours(12);
      expected.setMinutes(32);
      expected.setSeconds(50);
      expected.setMilliseconds(0);

      expect(roundTime(date, 1000 * 10).getTime()).to.be(expected.getTime());
    });

    it('should floor to the nearest 1 minute', function () {
      var date = new Date(0);
      date.setHours(12);
      date.setMinutes(32);
      date.setSeconds(54);
      date.setMilliseconds(40);

      var expected = new Date(0);
      expected.setHours(12);
      expected.setMinutes(32);
      expected.setSeconds(0);
      expected.setMilliseconds(0);

      expect(roundTime(date, 1000 * 60).getTime()).to.be(expected.getTime());
    });

    it('should floor to the nearest 5 minute', function () {
      var date = new Date(0);
      date.setHours(12);
      date.setMinutes(28);
      date.setSeconds(54);
      date.setMilliseconds(40);

      var expected = new Date(0);
      expected.setHours(12);
      expected.setMinutes(25);
      expected.setSeconds(0);
      expected.setMilliseconds(0);

      expect(roundTime(date, 1000 * 60 * 5).getTime())
        .to
        .be(expected.getTime());
    });

    it('should floor to the nearest 1 hour', function () {
      var date = new Date(0);
      date.setHours(12);
      date.setMinutes(28);
      date.setSeconds(54);
      date.setMilliseconds(40);

      var expected = new Date(0);
      expected.setHours(12);
      expected.setMinutes(0);
      expected.setSeconds(0);
      expected.setMilliseconds(0);

      expect(roundTime(date, 1000 * 60 * 60).getTime())
        .to
        .be(expected.getTime());
    });
  });
});