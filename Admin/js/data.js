$(function(){
  Configuration = getConf("../conf.json");
  getShowData(location.search.substring(1));
  
  $('.delete').on('click', function(){
    deleteRecord($('.id-span').text());
  });
  $('.change').on('click', function(){
    redirect("edit.html?recids="+$('.id-span').text());
  });
  $('.back').on('click', function(){
    parent.history.back();
    return false;
  });
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

function getShowData(data){
  var res = [];
  var rows = {rows:[]};
  var ajax = $.ajax({
    type: "GET", 
    url: Configuration.url+"?db="+Configuration.database+"&" + data,
    dataType: 'json',
    success: function (data) {
      if(!error(data)){
        $.each(data, function(key, val){
          $('.id-span').append(val[0]);
          rows.rows.push(val);
        })
        var theTemplateScript = Templates.show;  
         var theTemplate = Handlebars.compile(theTemplateScript);  
        $('.main-container').append(theTemplate(rows));
      }
      
    }
  });
}
