<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hello ${validation.username},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    Your data validation result can be viewed at this address:
    <br>
    <a href="${portalUrl}tools/data-validator/${validation.key}" style="color: #4ba2ce;text-decoration: none;">${portalUrl}tools/data-validator/${validation.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    <em>The GBIF Secretariat</em>
</p>

<#include "footer.ftl">
