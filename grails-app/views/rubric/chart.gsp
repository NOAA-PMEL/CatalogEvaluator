<!DOCTYPE html>
<html>
<head>
    <meta name="layout" content="main"/>
    <link rel="stylesheet" type="text/css" href="${resource(dir: 'css', file: 'evaluator.css')}">
    <link rel="stylesheet" type="text/css" href="${resource(dir: 'css/bootstrap/css', file: 'bootstrap.min.css')}">
    <g:set var="entityName" value="${message(code: 'rubric.label', default: 'Rubric')}"/>
    <title><g:message code="default.show.label" args="[entityName]"/></title>

    <script type='text/javascript' src='https://www.google.com/jsapi'></script>
    <script type='text/javascript' src="http://code.jquery.com/jquery-1.7.1.min.js"></script>

    <script type='text/javascript'>
        google.load('visualization', '1', {packages:['gauge']});
        google.load('visualization', '1', {packages:['corechart']});
    </script>

    <script type='text/javascript'>

        function charts() {

            var total = ${rubric.leaves}+${rubric.badLeaves};
            var percentGood = ( ${rubric.leaves}/total) * 100.;

            var percentAgg = 0;
            if ( ${rubric.aggregated} > 0 )
            {
                percentAgg = 100. - (((${rubric.leaves}-${rubric.aggregated}) /${rubric.leaves}) * 100.);
            }
            var percentServices = 0.;
            if (${rubric.services} > 0  )
            {
                percentServices = 100. - ((6 -${rubric.services}) / 6 * 100.);
            }
            var validTitle = "Valid Data (".concat(${rubric.leaves}," of ",total,")");
            drawCurrentChart(validTitle, 'Invalid Data', percentGood, 'chart_div1', 340, 340)
            drawCurrentChart('Aggregated', 'Not Aggregated', percentAgg, 'chart_div2', 340, 340)
            drawCurrentChart('Services', 'Missing Services', percentServices, 'chart_div3', 340, 340)
            if (percentServices < 100 && percentServices > 0) {
                document.getElementById("missingServices").style.visibility="visible"
            } else {
                document.getElementById("missingServices").style.visibility="hidden"
            }

              <g:each status="i" var="child" in="${children}">

                  var childTotal = (${child.leaves}+${child.badLeaves});
                  var childPGood = (${child.leaves}/childTotal) * 100.;
                  var childPAgg = 0;
                  if ( ${child.aggregated} > 0 )
                  {
                     childPAgg = 100. - (((${child.leaves}-${child.aggregated}) /${child.leaves}) * 100.);
                  }
                  var childPServices = 0.;
                  if (${child.services} > 0  )
                  {
                     childPServices = 100. - ((6 -${child.services}) / 6 * 100.);
                  }
                  var validChildTitle = 'Valid Data ('.concat(${child.leaves}," of ", childTotal , ")");
                  drawCurrentChart(validChildTitle, 'Invalid Data', childPGood, 'child_div1_${i}', 240, 240)
                  drawCurrentChart('Aggregated', 'Not Aggregated', childPAgg, 'child_div2_${i}', 240, 240)
                  drawCurrentChart('Services', 'Missing Services', childPServices, 'child_div3_${i}', 240, 240)
                  if (childPServices < 100 && childPServices > 0) {
                      document.getElementById("missingServicesChild_${i}").style.visibility="visible"
                 } else {
                     document.getElementById("missingServicesChild_${i}").style.visibility="hidden"
                 }

              </g:each>

        }



        function drawCurrentChart(label1, label2, value, myDiv, x, y) {


            if (label1.indexOf('Services') >= 0  && value == 0) {
                var data = google.visualization.arrayToDataTable([
                    [label1, label2],
                    [label1, 0],
                    [label2, 0],
                    [label1, 100]
                ]);
                var boptions = {
                    colors:['green','red','gray'],
                    is3D: 'true',
                    title: "Service Rubric does not apply to this catalog.",
                    legend: {position:'none'},
                    pieSliceText: 'none',
                    //chartArea: {left:10, width:'80%', height:'100%'},
                    tooltip: {trigger:'none'},
                    width:x,
                    height:y,
                    fontSize: 14.,
                    pieHole: 0.
                };
            } else {
                var data = google.visualization.arrayToDataTable([
                    [label1, label2],
                    [label1, value],
                    [label2, 100-value]
                ]);
                var boptions = {
                    colors:['green','red','gray'],
                    is3D: 'true',
                    title: label1,
                    legend: {position:'none'},
                    //chartArea: {left:10, width:'80%', height:'100%'},
                    tooltip: {text:'percentage'},
                    width:x,
                    height:y,
                    fontSize:14,
                    pieHole: 0.4
                };
            }
            var bchart = new google.visualization.PieChart(document.getElementById(myDiv));
            bchart.draw(data, boptions);

        }
        // Site Mesh or whatever renders these pages steals the body tag.
        // The JQuery ready function fires after all the page assets are loaded.
        jQuery(document).ready(function() {
            charts();
        });
    </script>


</head>

<body>
<a href="#show-rubric" class="skip" tabindex="-1"><g:message code="default.link.skip.label"
                                                             default="Skip to content&hellip;"/></a>


<div class="nav" role="navigation">
    <ul>
        <li><a class="home" href="${createLink(uri: '/')}"><g:message code="default.home.label"/></a></li>
    </ul>
</div>

<div id="show-rubric" class="content scaffold-show" role="main">
    <h1>Rubric [<g:link controller="catalog" action="top">List of top level catalogs</g:link>]</h1>
    <h2><g:if test="${rubric.catalog.parent.equals('none')}" ></g:if><g:else>
        [<g:link controller="rubric" action="chart" params="[parent: "${rubric?.catalog?.parentCatalog?.parent}", url:"${rubric?.catalog?.parent}"]">Parent</g:link>]</g:else>${rubric.catalog.url}  [<a href="${rubric.catalog.url.replace(".xml",".html")}">The original THREDDS Catalog</a>]</h2>
         <h3 class="evaldate">Evaluation as of ${rubric.lastUpdated}
             [<g:link controller="catalog" action="deleteAll" params="[parent: "${rubric?.catalog?.parent}", url:"${rubric?.catalog?.url}"]">Delete</g:link>]
             [<g:link controller="catalog" action="reEnter" params="[parent: "${rubric?.catalog?.parent}", url:"${rubric?.catalog?.url}"]">Re-evaluate</g:link>]</h3>
    <g:if test="${flash.message}">
        <div class="message" role="status">${flash.message}</div>
    </g:if>

</div>

<div class="container-fluid">
    <div class="row">
        <div class="col-md-4" id="chart_div1"></div>
        <div class="col-md-4" id="chart_div2">Column 2</div>
        <div class="col-md-4" id="chart_div3">Col 3</div>
    </div>
    <div class="row">
        <div class="col-md-4"><g:if test="${rubric.badLeaves>0 && rubric.catalog.leafCount>0}"><g:link controller="catalog" action="bad" params="[catid: "${rubric.catalog.id}"]">See errors</g:link></g:if></div>
        <div class="col-md-4"></div>
        <div class="col-md-4"><div class="label-info" id="missingServices">Missing Services: ${rubric.missingServices}</div></div>
    </div>
</div>
<div class="margin">
<g:each status="i" var="child" in="${children}">
    <h3>Rubric for catalog: <g:link controller="rubric" action="chart" params="[url:"${child?.catalog?.url}",parent:"${child?.catalog?.parent}"]">${child.catalog.url}</g:link> [<a href="${child.catalog.url.replace(".xml",".html")}">The original THREDDS Catalog</a>]
    <g:if test="${!child.missingServices.contains("UDDC")}">[<g:link controller="catalog" action="uddc" params="[catid: "${child.catalog.id}"]">See UDDC Metadata scores</g:link>]</g:if></h3>
    <div class="container-fluid">
        <div class="row">
            <div class="col-md-4" id="child_div1_${i}">col 1</div>
            <div class="col-md-4" id="child_div2_${i}">Column 2</div>
            <div class="col-md-4" id="child_div3_${i}">Col 3</div>
        </div>
        <div class="row">
            <div class="col-md-4"></div>
            <div class="col-md-4"></div>
            <div class="col-md-4"><div class="label-info" id="missingServicesChild_${i}">Missing Services: ${child.missingServices}</div></div>
        </div>
    </div>
</g:each>
</div>
</body>
</html>
