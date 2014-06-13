var DevicesListView = Backbone.View.extend({

  initialize: function (options) {
    this.options = options;
    this.$el.html(_.template($('#devices-list').html()));
    
    var self = this;

    this.$fromHourTextbox = this.$el.find('.from-hour-textbox');

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

        data.devices = JSON.stringify(devices);
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
        data.func =self.$aggregateFunctionSelector.val();
      }

      data.session = self.options.token;

      $.ajax({
        url: window.dbms + '/data/' + self.$seriesSwitcher.val(),
        type: 'GET',
        data: data
      }).done(function (data) {
        console.log(data);
      }).fail(function (xhr, status) {
        console.log(xhr.responseText);
      })
    });

    this.$aggregateFunctionSelector =
      this.$el.find('.aggregate-function-selector');

    $.ajax({
      url: window.dbms + '/series',
      data: { session: this.options.token }
    }).done(function (data, status, xhr) {
      data.forEach(function (series) {
        var $option = $(document.createElement('option'));
        $option.attr('value', series).html(series);
        self.$seriesSwitcher.append($option);
      });
      self.updateDevicesList();
    });
  },

  updateDevicesList: function () {
    var self = this;
    var url = '/devices/' + self.$seriesSwitcher.val();
    var data = { session: this.options.token };
    $.ajax({
      url: window.dbms + '/devices/' + self.$seriesSwitcher.val(),
      data: data
    }).success(function (data) {
      self.$devicesBox.html('');
      data.forEach(function (device) {
        var checkboxContainer = $(document.createElement('div'));
        checkboxContainer.html(
          _.template($('#device-checkbox').html(), {
            id: device.id,
            name: device.name || device.id
          })
        );
        self.$devicesBox.append(checkboxContainer);
      });
    });
  }

});

function login(callback) {
  callback = callback || function () {};

  var $dialog = $('#login-dialog');
  var $form = $dialog.find('.form');
  var $username = $form.find('.username');
  var $password = $form.find('.password');

  var loggedIn = false;

  var token = null;

  function logIn() {
    $.ajax({
      url: window.dbms + '/login',
      contentType: 'application/json',
      data: JSON.stringify({
        username: $username.val(),
        password: $password.val()
      }),
      method: 'POST'
    }).done(function (body) {
      token = body.token;
      loggedIn = true;
      $dialog.modal('hide');
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
      callback(token, $username.val(), $password.val());
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
  login(function (token, username, password) {
    $('#devices-list-view').append(new DevicesListView({
      username: username,
      password: password,
      token: token
    }).$el);
  });
});