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

function isRecord(str){
  return str.slice(0,2) == "r:";
}

function hasMoreThanNChars(str, count){
  var c = parseInt(count);
  if(str.length > c){
    return str.slice(0, c) + "...";
  }else{
    return str;
  }
}

Handlebars.registerHelper("substract", function(from, value){
  return parseInt(from) - parseInt(value);
});

Handlebars.registerHelper("append_new", function(value){
  return value;
});

Handlebars.registerHelper("is_record", function(value){
  var str = value.toString();
  if (isRecord(str)){
    return "<a href='/html/data.html?op=search&showid=yes&recids="+str.slice(2)+"'></a>";
  }else{
    return str;
  }
});

Handlebars.registerHelper("show_first_n", function(value, count){
  var str = value.toString();
  if (isRecord(str)){
    var record = str.slice(2);
    return "<a href='/html/data.html?op=search&showid=yes&recids="+record+"'>"+record+"</a>";
  }else{
    return hasMoreThanNChars(str, count);
  }
});