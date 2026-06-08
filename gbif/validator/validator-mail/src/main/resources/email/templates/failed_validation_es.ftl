<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hola ${validation.username},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Lamentamos informarles que ha occurrido un error procesdando sus datos.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Por más detalles, por favor consultar <a href="${portalUrl}es/tools/data-validator/${validation.key}" style="color: #4ba2ce;text-decoration: none;">${portalUrl}es/tools/data-validator/${validation.key}</a> <br>
  Consulte el estado de los servicios de GBIF en <a href="${portalUrl}es/system-health" style="color: #4ba2ce;text-decoration: none;">${portalUrl}es/system-health</a>, e intente de nuevo en unos minutos.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  i el problema persiste, contáctenos utilizando la funcionalidad de retroalimentación del sitio web, ó escribiendo a <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>.<br>
    Por favor incluya la identificación (${validation.key}) de la validación fallida.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>The GBIF Secretariat</em>
</p>

<#include "footer.ftl">
