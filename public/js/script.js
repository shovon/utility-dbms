var DevicesListView = Backbone.View.extend({

  initialize: function () {
    this.$el.html(_.template($('#devices-list').html()));
    
    var self = this;

    this.$devicesBox = this.$el.find('.devices-box');

    this.$seriesSwitcher = this.$el.find('.series-switcher');
    this.$seriesSwitcher.change(function () {
      self.updateDevicesList();
    });

    this.$queryButton = this.$el.find('.query');
    this.$queryButton.click(function () {
      var checked = self.$devicesBox.find('input[type="checkbox"]:checked');
      console.log(self.$devicesBox[0]);
      var data = {};
      console.log(self.$devicesBox.find('input[type="checkbox"]').length);
      console.log(checked.length);
      if (self.$devicesBox.find('input[type="checkbox"]').length != checked.length) {
        // TODO: add a way to check whether or not the user intends to exclude
        //   the specified devices.
        data.devices = JSON.stringify({
          ids: $.makeArray(checked.map(function (i, box) {
            return $(box).attr('data-device-id');
          }))
        })
      }
      console.log(data);
      $.ajax({
        url: '/data/' + self.$seriesSwitcher.val(),
        type: 'GET',
        data: data
      }).done(function (data) {
        throw new Error('Shouldn\'t be here.');
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