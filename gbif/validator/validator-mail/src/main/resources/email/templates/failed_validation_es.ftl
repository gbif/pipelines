<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<#assign systemHealthUrl>${portalUrl?replace("/+$", "", "r")}/es/system-health</#assign>
<#assign validationUrl>${validatorUrl?replace("/+$", "", "r")}/es/tools/data-validator/${validation.key}</#assign>

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hola ${validation.username},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Lamentamos informarles que ha occurrido un error procesdando sus datos.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Por más detalles, por favor consultar <a href="${validationUrl}" style="color: #4ba2ce;text-decoration: none;">${validationUrl}</a> <br>
  Consulte el estado de los servicios de GBIF en <a href="${systemHealthUrl}" style="color: #4ba2ce;text-decoration: none;">${systemHealthUrl}</a>, e intente de nuevo en unos minutos.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  i el problema persiste, contáctenos utilizando la funcionalidad de retroalimentación del sitio web, ó escribiendo a <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>.<br>
    Por favor incluya la identificación (${validation.key}) de la validación fallida.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>The GBIF Secretariat</em>
</p>

<#include "footer.ftl">
