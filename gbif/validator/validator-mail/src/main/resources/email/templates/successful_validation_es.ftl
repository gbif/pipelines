<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">HOLA ${validation.username},</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  El resultado de su validación de datos puede ser consultado en la siguiente dirección:
  <br>
  <a href="${portalUrl}tools/data-validator/${validation.key}" style="color: #509E2F;text-decoration: none;">${portalUrl}tools/data-validator/${validation.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Secretariado de GBIF</em>
</p>

<#include "footer.ftl">
