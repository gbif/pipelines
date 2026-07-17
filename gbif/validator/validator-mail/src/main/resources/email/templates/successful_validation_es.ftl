<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hola ${validation.username},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  El resultado de su validación de datos puede ser consultado en la siguiente dirección:
  <br>
  <a href="${portalUrl}tools/data-validator/${validation.key}" style="color: #4ba2ce;text-decoration: none;">${portalUrl}tools/data-validator/${validation.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Secretariado de GBIF</em>
</p>

<#include "footer.ftl">
