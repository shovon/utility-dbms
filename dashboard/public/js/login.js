function login() {
  var $dialog = $('#login-dialog');
  var $form = $dialog.find('form');
  var $username = $form.find('.username');
  var $password = $form.find('.password');
  
  $form.on('submit', function (e) {
    e.stopPropagation();
    e.preventDefault();
  });

  $dialog.on('hidden.bs.modal', function (e) {
    $.ajax({
      url: window.dbms + '/login',
      data
    })
  });
}