$(function(){
    Configuration = getConf("../conf.json");
    var w = $(window).width(), h = $(window).height();

    var labelDistance = 0;

    var vis = d3.select("body").append("svg:svg").attr("width", w).attr("height", h);

    var nodes = [];
    var labelAnchors = [];
    var labelAnchorLinks = [];
    var links = [];
    $.ajax({
      type: 'GET',
      url: Configuration.url+'?db='+Configuration.database+'&op=search&showid=yes',
      dataType: 'json',
      async: false,
      success: function(data){
        for(var i = 0; i < data.length; i++) {
          var element = data[i];
          var node = {
            label : element[0],
            targets : []
          };
          
          for (var j = 1; j < element.length; j++) {
            if(toString(element[j]).slice(0,1)=="r"){
              node.targets.push(element[j].slice(2))
            }
          };

          nodes.push(node);
          labelAnchors.push({
            node : node
          });
          labelAnchors.push({
            node : node
          });
        };

        for(var i = 0; i < nodes.length; i++) {
          for(var j = 0; j < nodes.length; j++) {
            var node = nodes[i];
            var targets = nodes[j].targets;
            for(var t=0; t < targets.length; t++){
              if(node.label==targets[t]){
                links.push({
                  source : i,
                  target : j,
                  weight : Math.random()
                });
              }  
            }
              
          }
          labelAnchorLinks.push({
            source : i*2,
            target : i*2+1,
            weight : 1
          });
        };

        var force = d3.layout.force().size([w, h]).nodes(nodes).links(links).gravity(1).linkDistance(100).charge(-3000).linkStrength(function(x) {
          return x.weight * 10
        });


        force.start();

        var force2 = d3.layout.force().nodes(labelAnchors).links(labelAnchorLinks).gravity(0).linkDistance(0).linkStrength(9).charge(-100).size([w, h]);
        force2.start();

        var link = vis.selectAll("line.link").data(links).enter().append("svg:line").attr("class", "link").style("stroke", "#000");

        var node = vis.selectAll("g.node").data(force.nodes()).enter().append("svg:g").attr("class", "node");
        node.append("svg:circle").attr("r", 7).style("fill", "#000").style("stroke", "#000").style("stroke-width", 4);
        node.call(force.drag);


        var anchorLink = vis.selectAll("line.anchorLink").data(labelAnchorLinks)//.enter().append("svg:line").attr("class", "anchorLink").style("stroke", "#999");

        var anchorNode = vis.selectAll("g.anchorNode").data(force2.nodes()).enter().append("svg:g").attr("class", "anchorNode");
        anchorNode.append("svg:circle").attr("r", 0).style("fill", "#000");
          anchorNode.append("svg:text").text(function(d, i) {
          return i % 2 == 0 ? "" : d.node.label
        }).style("fill", "#000").style("font-family", "Arial").style("font-size", 20);

        var updateLink = function() {
          this.attr("x1", function(d) {
            return d.source.x;
          }).attr("y1", function(d) {
            return d.source.y;
          }).attr("x2", function(d) {
            return d.target.x;
          }).attr("y2", function(d) {
            return d.target.y;
          });
        }

        var updateNode = function() {
          this.attr("transform", function(d) {
            return "translate(" + d.x + "," + d.y + ")";
          });
        }


        force.on("tick", function() {

          force2.start();

          node.call(updateNode);

          anchorNode.each(function(d, i) {
            if(i % 2 == 0) {
              d.x = d.node.x;
              d.y = d.node.y;
            } else {
              var b = this.childNodes[1].getBBox();

              var diffX = d.x - d.node.x;
              var diffY = d.y - d.node.y;

              var dist = Math.sqrt(diffX * diffX + diffY * diffY);

              var shiftX = b.width * (diffX - dist) / (dist * 2);
              shiftX = Math.max(-b.width, Math.min(0, shiftX));
              var shiftY = 5;
              this.childNodes[1].setAttribute("transform", "translate(" + shiftX + "," + shiftY + ")");
            }
          });


          anchorNode.call(updateNode);

          link.call(updateLink);
          anchorLink.call(updateLink);

        });
      }
    })
})