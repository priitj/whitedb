function error(str){
  if(str.indexOf("ERROR")>-1){
    $('.error-div').html(str);
    $('.main-container').hide();
    return true;
  }else{
    $('.error-div').hide();
    return false;
  }
}

function getConf(url){
  var conf = {};
  $.ajax({
    url: url,
    async: false,
    success: function(data){
      conf = data;
    }
  });
  return conf;
}

function updateData(formData){

}

function redirect(url){
  window.location = url;
}

Handlebars.registerHelper("substract", function(from, value){
  return parseInt(from) - parseInt(value);
});

Handlebars.registerHelper("append_new", function(value){
  return value;
});

Handlebars.registerHelper("is_record", function(value){
  var str = value.toString();
  if (str.slice(0,2)=="r:"){
    return "<a href='/html/data.html?op=search&showid=yes&recids="+str.slice(2)+"'></a>";
  }else{
    return str;
  }
});