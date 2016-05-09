<%--
  Created by IntelliJ IDEA.
  User: rhs
  Date: 8/12/15
  Time: 2:18 PM
--%>
<%@ page import="pmel.sdig.cleaner.Catalog" %>
<%@ page contentType="text/html;charset=UTF-8" %>
<!DOCTYPE html>
<html>
<head>
    <meta name="layout" content="main">

    <g:set var="entityName"
           value="${message(code: 'catalog.url', default: 'Catalog')}" />
    <title><g:message code="default.create.label"
                      args="[entityName]" /></title>
    <script>
        var auto_refresh = setInterval(function() { submitform(); }, 5000);
        function submitform(){

            if ("${catalogInstance?.url}" != "" ) {

                document.forms.namedItem("enter").submit();
            }

        }
    </script>
</head>
<body>
<a href="#create-catalog" class="skip" tabindex="-1"><g:message
        code="default.link.skip.label" default="Skip to content&hellip;" /></a>

<div id="create-catalog" class="content scaffold-create" role="main">
    <h1>
        <g:if test="${catalogInstance}">

            Evaluating ${catalogInstance.url}

        </g:if>
        <g:else>

            Enter THREDDS Catalog URL to Evaluate


        </g:else>

    </h1>
    <g:if test="${flash.message}">
        <div class="message" role="status">
            ${flash.message}
        </div>
    </g:if>
    <g:if test="${flash.error}">
        <div class="errors" role="error">
            <ul>
                <li>${flash.error}</li>
            </ul>
        </div>
    </g:if>
    <g:hasErrors bean="${catalogInstance}">
        <ul class="errors" role="alert">
            <g:eachError bean="${catalogInstance}" var="error">
                <li
                    <g:if test="${error in org.springframework.validation.FieldError}">data-field-id="${error.field}"></g:if><g:message
                        error="${error}" /></li>
            </g:eachError>
        </ul>
    </g:hasErrors>
    <g:form id="enter" url="[resource:catalogInstance, action:'enter']">
        <fieldset class="form">

            <div>
                <label for="url"> <g:message code="catalog.url.label"             default="Url" />

                </label>
                <g:textField name="url" value="${catalogInstance?.url}" />

            </div>
        </fieldset>
        <fieldset class="buttons">
            <g:submitButton name="evaluate" class="evaluate" value="Evaluate" />
        </fieldset>
    </g:form>
</div>
</body>
</html>
