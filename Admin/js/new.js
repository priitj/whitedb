$(function(){
  Configuration = getConf("../conf.json");
  var i = -1;
  $('.add-field').on('click', function(event){
    event.preventDefault();
    if($('.new-field-data').val()==""){
      $('.new-field-data').addClass("field-error");
      $('.error-span').html('Enter a value');
      return false;
    }else{
      var rows = {rows: []}
      var theTemplateScript = Templates.add_new_field;  
       var theTemplate = Handlebars.compile(theTemplateScript);  
      
      rows.rows.push($('.new-field-data').val());
      $('.new-row-form').append(theTemplate(rows));

      i++;
      $('.field-label').last().html(i);
      $('.new-field-data').val("");
    }
  });
  $('.new-row-form').on('click', '.remove-item', function(event){
    event.preventDefault();
    $(this).parent('.form-group').remove();
    i = 0;
    $.each($('.field-label'), function(){
      $(this).html(i++);
    });
  });
  $('.create').on('click', function(){
    var data = "rec=";
    $.each($('.field'), function(){
      data += $(this).val()+",";
      console.log($(this).val());
    })
    console.log(data.slice(0, -1));
  });
  $('.new-field-data').on('click', function(){
    $(this).removeClass('field-error');
    $('.error-span').html('');
  })
});