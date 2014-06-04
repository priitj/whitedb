Templates = {}
Templates.index_view = [
  "<table class='index-table table footable ' data-page-size='1000' data-page-navigation='.pagination'>",
    "<thead>",
      "<tr>",
        "<th>id</th>",
        "<th>0</th>",
        "<th>1</th>",
        "<th>2</th>",
        "<th>3</th>",
        "<th>4</th>",
        "<th></th>",
        "<th>Delete</th>",
      "</tr>",
    "</thead>",
    "<tbody>",
      "{{#each rows}}",
        "<tr data-id={{this.[0]}}>",
          "{{#each this}}",
              "<td class='index-pointer'>",
                "{{#show_first_n this 10}}",
                  "this",
                "{{/show_first_n}}",
          "{{/each}}",
          "<td class='delete index-pointer'><span class='glyphicon glyphicon-remove delete'></span></td>",
        "</tr>",
      "{{/each}}",
    "</tbody>",
  "</table>"
].join('\n')

Templates.show = [
  "<table class='index-table table'>",
    "{{#each rows}}",
        "{{#each this}}",
          "{{#is @index '>' 0}}",
            "<tr>",
              "<th>{{#substract @index 1}}{{/substract}}</th>",
              "<td>{{#is_record this}}{{/is_record}}</td>",
            "</tr>",
          "{{/is}}",
        "{{/each}}",
    "{{/each}}",
  "</table>"
].join('\n')

Templates.edit = [
  "<form class='form-horizontal edit-form'>",
    "{{#each rows}}",
        "{{#each this}}",
          "{{#is @index '>' 0}}",
            "<div class='form-group'>",
              "<label class='col-sm-2 control-label margin-right'>",
                "{{#substract @index 1}}",
                "{{/substract}}",
              "</label>",
              "<div class='col-sm-8'>",
                "<input class='form-control' name='ufield' value='{{this}}'/>",
                "<input class='form-control' style='display: none;' name='uvalue' value='{{@index}}'/>",
              "</div>",
            "</div>",
          "{{/is}}",
        "{{/each}}",
    "{{/each}}",
  "</form>"
].join('\n')


Templates.add_new_field = [
  "<div class='form-group'>",
    "{{#each rows}}",
      "<label class='control-label col-sm-1 margin-right field-label'></label>",
      "<div class='col-sm-4'>",
        "<input class='form-control field' type='text' value='{{this}}'>",
      "</div>",
      "<div class='col-md-1'>",
        "<select class='form-control' name='type'>",
          "<option value='s' selected>str</option>",
          "<option >int</option>",
          "<option>double</option>",
          "<option>char</option>",
          "<option value='r'>record</option>",
        "</select>",
      "</div>",
      "<span class='btn btn-link glyphicon glyphicon-remove remove-item'></span>",
    "{{/each}}",
  "</div>"
].join('\n')