<!DOCTYPE html>
<html>
<head>
    <meta name="layout" content="main"/>
    <g:set var="entityName" value="${message(code: 'catalog.label', default: 'Catalog')}"/>
    <link rel="stylesheet" type="text/css" href="${resource(dir: 'css', file: 'evaluator.css')}">
    <title>Error messages:</title>
</head>

<body>
<a href="#list-catalog" class="skip" tabindex="-1"><g:message code="default.link.skip.label"
                                                              default="Skip to content&hellip;"/></a>

<div class="nav" role="navigation">
    <ul>
        <li><a class="home" href="${createLink(uri: '/')}"><g:message code="default.home.label"/></a></li>
    </ul>
</div>

<div id="errors" class="content" role="main">
    <g:if test="${flash.message}">
        <div class="message" role="status">${flash.message}</div>
    </g:if>
    <div class="header-spacing">

        <g:each status="i" var="dataset" in="${datasets}">

            <h3>Error report for: ${dataset.url}</h3>
            <g:if test="${!dataset.error.equals('none')}"><br>${dataset.error}</g:if>
            <g:if test="${dataset.badNetCDFVariables != null && !dataset.badNetCDFVariables.isEmpty() }">
                <table class="header-spacing">
                    <tr>
                        <th>Variable:</th><th>Error:</th>
                    </tr>
                    <g:each status="j" var="bad" in="${dataset.badNetCDFVariables}">
                        <tr>
                            <td>${bad.name}</td>  <td>${bad.error}</td>
                        </tr>
                    </g:each>
                </table>
            </g:if>
        </g:each>
    </div>
</div>
</body>
</html>