<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<#assign validationUrl>${validatorUrl?replace("/+$", "", "r")}/tools/data-validator/${validation.key}</#assign>

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hello ${validation.username},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    Your data validation result can be viewed at this address:
    <br>
    <a href="${validationUrl}" style="color: #4ba2ce;text-decoration: none;">${validationUrl}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    <em>The GBIF Secretariat</em>
</p>

<#include "footer.ftl">
