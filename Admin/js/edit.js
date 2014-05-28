$(function(){
  Configuration = getConf("../conf.json");
  getEditData(location.search.substring(1).split('&'));
  $('.back').on('click', function(){
    parent.history.back();
    return false;
  });
  $('.save').on('click', function(){
    updateData($('.edit-form').serialize());
  })
});

function deleteRecord(id){
  $.ajax({
    type: 'GET',
    url: Configuration.url+"?db="+Configuration.database+"&op=delete&recids=" + id,
    dataType: 'json',
    success: function(data){
      $(location).attr('href',"../index.html");
    }
  });
}

function getEditData(dataId){
  var res = [];
  var rows = {rows:[]};
  var ajax = $.ajax({
    type: "GET", 
    url: Configuration.url+"?db="+Configuration.database+"&op=search&showid=yes&"+dataId,
    dataType: 'json',
    success: function (data) {
      console.log(data);
      if (data.length > 0) {
        if(!error(data)){
          $.each(data, function(key, val){
            $('.id-span').append(val[0]);
            rows.rows.push(val);
          });
          var theTemplateScript = Templates.edit;  
           var theTemplate = Handlebars.compile(theTemplateScript);  
          $('.main-container').append(theTemplate(rows));
        }
      }else{
        $('.error-div').html("No record found");
        $('.edit-container').hide();
      };
    }
  });
}
