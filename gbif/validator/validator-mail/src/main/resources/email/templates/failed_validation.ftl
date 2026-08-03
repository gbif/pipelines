<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<#assign systemHealthUrl>${portalUrl?replace("/+$", "", "r")}/system-health</#assign>
<#assign validationUrl>${validatorUrl?replace("/+$", "", "r")}/tools/data-validator/${validation.key}</#assign>

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hello ${validation.username},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    We are sorry, but an error has occurred while processing your data.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    Please see <a href="${validationUrl}" style="color: #4ba2ce;text-decoration: none;">${validationUrl}</a> for more details, <a href="${systemHealthUrl}" style="color: #4ba2ce;text-decoration: none;">${systemHealthUrl}</a> for the current status of GBIF.org's systems, and try again in a few minutes.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    If the problem persists, contact us using the feedback system on the website, or at <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>.  Please include the validation key (${validation.key}) of the failed validation.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    <em>The GBIF Secretariat</em>
</p>

<#include "footer.ftl">
