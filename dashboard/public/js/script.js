var DevicesListView = Backbone.View.extend({

  initialize: function (options) {
    this.options = options;
    this.$el.html(_.template($('#devices-list').html()));
    
    var self = this;

    this.$fromTimeTextbox = this.$el.find('.from-time-textbox');
    this.$toTimeTextbox = this.$el.find('.to-time-textbox');

    this.$devicesBox = this.$el.find('.devices-box');

    this.$seriesSwitcher = this.$el.find('.series-switcher');
    this.$seriesSwitcher.change(function () {
      self.updateDevicesList();
    });

    this.$excludeCheckbox = this.$el.find('.exclude-checkbox');

    this.$granularitySelector = this.$el.find('.granularity-selector');

    this.$unitsTextbox = this.$el.find('.units-textbox');
    this.$unitsTextbox.keyup(function () {
      var $this = $(this);
      var value = +$this.val()
      if (isNaN(value) || value < 1) {
        $this.val('1');
      }
    });

    this.$queryButton = this.$el.find('.query');
    this.$queryButton.click(function () {
      var checked = self.$devicesBox.find('input[type="checkbox"]:checked');
      var data = {};
      if (self.$devicesBox.find('input[type="checkbox"]').length != checked.length) {
        var devices = {
          ids: $.makeArray(checked.map(function (i, box) {
            return $(box).attr('data-device-id');
          }))
        };

        if (self.$excludeCheckbox.is(':checked')) {
          devices.exclude = true;
        }

        data.devices = devices;
      }

      if (
        +self.$unitsTextbox.val() !== 1 ||
        self.$granularitySelector.val() !== 'none'
      ) {
        data.interval =
          +self.$unitsTextbox.val();
        if (self.$granularitySelector.val() !== 'none') {
          data.interval += self.$granularitySelector.val();
        }
      }

      if (/(min|max|sum)/.test(self.$aggregateFunctionSelector.val())) {
        data.func = self.$aggregateFunctionSelector.val();
      }

      if (/(mean|min|max|sum)/.test(self.$groupbyhourSelector.val())) {
        data.groupbyhour = self.$groupbyhourSelector.val();
      }

      if (self.$fromTimeTextbox.val()) {
        data.from = self.$fromTimeTextbox.val();
      }

      if (self.$toTimeTextbox.val()) {
        data.to = self.$toTimeTextbox.val();
      }

      data.session = self.options.token;

      self.options.dbmsclient.getData(
        self.$seriesSwitcher.val(),
        data,
        function (err, data) {
          if (err) {
            return alert('Failed to get data. :(');
          }
          drawGraph(data);
        }
      );
    });

    this.$aggregateFunctionSelector =
      this.$el.find('.aggregate-function-selector');

    this.$groupbyhourSelector = this.$el.find('.groupbyhour-selector');

    this.options.dbmsclient.getSeries(function (err, series) {
      if (err) {
        return alert('Some error occurred. :(');
      }
      series.forEach(function (series) {
        var $option = $(document.createElement('option'));
        $option.attr('value', series).html(series);
        self.$seriesSwitcher.append($option);
      });
      self.updateDevicesList();
    });
  },

  updateDevicesList: function () {
    var self = this;
    var data = { session: this.options.token };
    this.options.dbmsclient.getDevicesForSeries(
      self.$seriesSwitcher.val(),
      function (err, devices) {
        if (err) {
          return alert('Some error occurred. :(');
        }
        self.$devicesBox.html('');
        devices.forEach(function (device) {
          var checkboxContainer = $(document.createElement('div'));
          checkboxContainer.html(
            _.template($('#device-checkbox').html(), {
              id: device.id,
              name: device.name || device.id
            })
          );
          self.$devicesBox.append(checkboxContainer);
        });
      }
    );
  }

});

function login(callback) {
  callback = callback || function () {};

  var $dialog = $('#login-dialog');
  var $form = $dialog.find('.form');
  var $username = $form.find('.username');
  var $password = $form.find('.password');
  var dbmsclient = null;

  var loggedIn = false;

  function logIn() {
    dbmsclient = new DBMSClient($username.val(), $password.val(), window.dbms);
    dbmsclient.login(function (err) {
      if (!err) {
        loggedIn = true;
        $dialog.modal('hide');
      }
    });
  }

  $form.find('.username, .password').keyup(function () {
    if (event.keyCode == 13) {
      logIn();
    }
  });
  
  $form.on('submit', function (e) {
    e.stopPropagation();
    e.preventDefault();
  });

  $dialog.on('hidden.bs.modal', function (e) {
    if (!loggedIn) {
      $dialog.modal({});
    } else {
      callback(dbmsclient);
    }
  });

  $dialog.find('.log-in').click(function (e) {
    e.stopPropagation();
    e.preventDefault();
    logIn();
  });

  $dialog.modal({});
}

$(function () {
  login(function (dbmsclient) {
    $('#devices-list-view').append(new DevicesListView({
      dbmsclient: dbmsclient
    }).$el);
  });
});