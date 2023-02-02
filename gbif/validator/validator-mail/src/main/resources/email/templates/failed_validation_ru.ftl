<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Hello ${validation.username},
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  We are sorry, but an error has occurred processing your data.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Please see <a href="${portalUrl}validation${validation.key}" style="color: #509E2F;text-decoration: none;">${portalUrl}tools/data-validator/${validation.key}</a> for more details, <a href="${portalUrl}ru/system-health" style="color: #509E2F;text-decoration: none;">${portalUrl}ru/system-health</a> for the current status of GBIF.org's systems, and try again in a few minutes.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  If the problem persists, contact us using the feedback system on the website, or at <a href="mailto:helpdesk@gbif.org" style="color: #509E2F;text-decoration: none;">helpdesk@gbif.org</a>.  Please include the validation key (${validation.key}) of the failed validation.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>The GBIF Secretariat</em>
</p>

<#include "footer.ftl">
