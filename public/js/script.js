var DevicesListView = Backbone.View.extend({

  initialize: function () {
    this.$el.html(_.template($('#devices-list').html()));
    
    var self = this;

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
        console.log(data.interval);
      }

      $.ajax({
        url: '/data/' + self.$seriesSwitcher.val(),
        type: 'GET',
        data: data
      }).done(function (data) {
        console.log(data);
      }).fail(function (xhr, status) {
        console.log(xhr.responseText);
      })
    });

    $.ajax({
      url: '/series'
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
    $.ajax({
      url: '/devices/' + self.$seriesSwitcher.val()
    }).success(function (data) {
      self.$devicesBox.html('');
      data.forEach(function (device) {
        var checkboxContainer = $(document.createElement('div'));
        var checkbox = $(document.createElement('input'));
        checkbox.attr('type', 'checkbox');
        checkbox.attr('checked', 'true');
        checkbox.attr('data-device-id', device.id);
        var span = $(document.createElement('span'));
        span.html(device.id);
        checkboxContainer.append(checkbox);
        checkboxContainer.append(span);
        self.$devicesBox.append(checkboxContainer);
      });
    });
  }

});

$(function () {
  $('#devices-list-view').append(new DevicesListView().$el);
});