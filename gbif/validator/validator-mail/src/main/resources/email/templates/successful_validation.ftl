<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">Hello ${validation.username},</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Your data validation result can be consulted at this address:
  <br>
  <a href="${portalUrl}validation/${validation.key}" style="color: #509E2F;text-decoration: none;">${portalUrl}validation/${validation.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>The GBIF Secretariat</em>
</p>

<#include "footer.ftl">
