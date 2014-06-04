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
      $.ajax({
        url: '/data/' + self.$seriesSwitcher.val()
      }).done(function (data) {
        throw new Error('Shouldn\'t be here.');
      }).fail(function (xhr, status) {
        console.log(xhr);
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