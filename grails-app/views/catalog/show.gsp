<!DOCTYPE html>
<html>
<head>
    <meta name="layout" content="main"/>
    <g:set var="entityName" value="${message(code: 'catalog.label', default: 'Catalog')}"/>
    <title><g:message code="default.show.label" args="[entityName]"/></title>
</head>

<body>
<a href="#show-catalog" class="skip" tabindex="-1"><g:message code="default.link.skip.label"
                                                              default="Skip to content&hellip;"/></a>

<div class="nav" role="navigation">
    <ul>
        <li><a class="home" href="${createLink(uri: '/')}"><g:message code="default.home.label"/></a></li>
        <li><g:link class="list" action="index"><g:message code="default.list.label" args="[entityName]"/></g:link></li>
        <li><g:link class="create" action="create"><g:message code="default.new.label"
                                                              args="[entityName]"/></g:link></li>
    </ul>
</div>

<div id="show-catalog" class="content scaffold-show" role="main">
    <h1><g:message code="default.show.label" args="[entityName]"/></h1>
    <g:if test="${flash.message}">
        <div class="message" role="status">${flash.message}</div>
    </g:if>
    <li class="fieldcontain">
        <g:link controller="catalog" action="crawldata" params="[parent:"${catalog?.parent}", url:"${catalog?.url}"]">Scan Data Sources in this Catalog and its children.</g:link>
    </li>
    <li class="fieldcontain">
        <g:link controller="catalog" action="clean" params="[parent:"${catalog?.parent}", url:"${catalog?.url}"]">Clean this Catalog and its children.</g:link>
    </li>
    <li class="fieldcontain">
        <g:link controller="catalog" action="deleteAll" params="[parent:"${catalog?.parent}", url:"${catalog?.url}"]" style="font-size: medium;color: red;">Delete this Catalog and its children.</g:link>
    </li>
    <f:display bean="catalog"/>
    <g:form resource="${this.catalog}" method="DELETE">
        <fieldset class="buttons">
            <g:link class="edit" action="edit" resource="${this.catalog}"><g:message code="default.button.edit.label"
                                                                                     default="Edit"/></g:link>
            <input class="delete" type="submit"
                   value="${message(code: 'default.button.delete.label', default: 'Delete')}"
                   onclick="return confirm('${message(code: 'default.button.delete.confirm.message', default: 'Are you sure?')}');"/>
        </fieldset>
    </g:form>
</div>
</body>
</html>
