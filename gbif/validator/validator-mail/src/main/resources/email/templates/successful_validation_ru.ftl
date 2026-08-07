<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<#assign validationUrl>${validatorUrl?replace("/+$", "", "r")}/tools/data-validator/${validation.key}</#assign>

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Уважаемый пользователь ${validation.username},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    Результаты проверки ваших данных доступны по следующему адресу:
    <br>
    <a href="${validationUrl}" style="color: #4ba2ce;text-decoration: none;">${validationUrl}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    <em>Секретариат GBIF</em>
</p>

<#include "footer.ftl">
