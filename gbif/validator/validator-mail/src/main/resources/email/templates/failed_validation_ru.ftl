<#-- @ftlvariable name="" type="org.gbif.mail.validator.ValidatorTemplateDataModel" -->
<#include "header.ftl">

<#assign systemHealthUrl>${portalUrl?replace("/+$", "", "r")}/ru/system-health</#assign>
<#assign validationUrl>${validatorUrl?replace("/+$", "", "r")}/ru/tools/data-validator/${validation.key}</#assign>

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Уважаемый пользователь ${validation.username},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    К сожалению, при обработке ваших данных произошла ошибка.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    Подробнее см. <a href="${validationUrl}" style="color: #4ba2ce;text-decoration: none;">${validationUrl}</a>, текущий статус систем GBIF.org доступен на странице <a href="${systemHealthUrl}" style="color: #4ba2ce;text-decoration: none;">${systemHealthUrl}</a>. Пожалуйста, повторите попытку через несколько минут.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    Если проблема сохраняется, свяжитесь с нами через систему обратной связи на сайте или по адресу <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>. Пожалуйста, укажите идентификатор (${validation.key}) неудачной проверки.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
    <em>Секретариат GBIF</em>
</p>

<#include "footer.ftl">
