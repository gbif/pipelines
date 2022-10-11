package org.gbif.converters.parser.xml.parsing.xml.rules;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.RuleSet;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
import org.gbif.converters.parser.xml.parsing.xml.PrioritizedProperty;

@Slf4j
public abstract class AbstractRuleSet implements RuleSet {

  protected Properties mappingProps;

  @Override
  public void addRuleInstances(Digester digester) {
    // common, simple fields
    addNonNullMethod(digester, "institutionCode", "setInstitutionCode", 1);
    addNonNullParam(digester, "institutionCode", 0);

    addNonNullMethod(digester, "collectionCode", "setCollectionCode", 1);
    addNonNullParam(digester, "collectionCode", 0);

    addNonNullMethod(digester, "basisOfRecord", "setBasisOfRecord", 1);
    addNonNullParam(digester, "basisOfRecord", 0);

    addNonNullMethod(digester, "minDepth", "setMinDepth", 1);
    addNonNullParam(digester, "minDepth", 0);

    addNonNullMethod(digester, "maxDepth", "setMaxDepth", 1);
    addNonNullParam(digester, "maxDepth", 0);

    addNonNullMethod(digester, "latLongPrecision", "setLatLongPrecision", 1);
    addNonNullParam(digester, "latLongPrecision", 0);

    addNonNullMethod(digester, "minAltitude", "setMinAltitude", 1);
    addNonNullParam(digester, "minAltitude", 0);

    addNonNullMethod(digester, "maxAltitude", "setMaxAltitude", 1);
    addNonNullParam(digester, "maxAltitude", 0);

    // identifier records
    addNonNullMethod(digester, "identifierType1", "setIdentifierType1", 1);
    addNonNullParam(digester, "identifierType1", 0);

    addNonNullMethod(digester, "identifierType2", "setIdentifierType2", 1);
    addNonNullParam(digester, "identifierType2", 0);

    addNonNullMethod(digester, "identifierType3", "setIdentifierType3", 1);
    addNonNullParam(digester, "identifierType3", 0);

    addNonNullMethod(digester, "identifierType4", "setIdentifierType4", 1);
    addNonNullParam(digester, "identifierType4", 0);

    addNonNullMethod(digester, "identifierType5", "setIdentifierType5", 1);
    addNonNullParam(digester, "identifierType5", 0);

    addNonNullMethod(digester, "identifierType6", "setIdentifierType6", 1);
    addNonNullParam(digester, "identifierType6", 0);

    addNonNullMethod(digester, "identifierType7", "setIdentifierType7", 1);
    addNonNullParam(digester, "identifierType7", 0);

    // possibly many typifications
    addNonNullMethod(digester, "typificationElement", "addTypification", 4);
    addNonNullParam(digester, "typeScientificName", 0);
    addNonNullParam(digester, "typePublication", 1);
    addNonNullParam(digester, "typeStatus", 2);
    addNonNullParam(digester, "typeNotes", 3);
  }

  protected void addNonNullMethod(
      Digester digester, String property, String methodName, int argCount) {
    String pattern = mappingProps.getProperty(property);
    if (pattern != null) {
      pattern = pattern.trim();
      log.debug(
          "adding call method [{}] for pattern [{}] from property [{}]",
          methodName,
          pattern,
          property);
      digester.addCallMethod(pattern, methodName, argCount);
    }
  }

  protected void addNonNullParam(Digester digester, String property, int argPosition) {
    String pattern = mappingProps.getProperty(property);
    if (pattern != null) {
      pattern = pattern.trim();
      log.debug("adding call param for pattern [{}] from property [{}]", pattern, property);
      digester.addCallParam(pattern, argPosition);
    }
  }

  protected void addNonNullAttParam(
      Digester digester, String elementProperty, String attributeProperty, int argPosition) {
    String elemPattern = mappingProps.getProperty(elementProperty);
    String attPattern = mappingProps.getProperty(attributeProperty);
    if (elemPattern != null && attPattern != null) {
      elemPattern = elemPattern.trim();
      attPattern = attPattern.trim();
      log.debug(
          "adding call param from attribute for element pattern [{}] from property [{}], seeking attribute [{}] from property [{}]",
          elemPattern,
          elementProperty,
          attPattern,
          attributeProperty);
      digester.addCallParam(elemPattern, argPosition, attPattern);
    }
  }

  protected void addNonNullPrioritizedProperty(
      Digester digester, String property, PrioritizedPropertyNameEnum name, int paramCount) {
    for (int i = 1; i <= paramCount; i++) {
      String key = property + '.' + i;
      String pattern = mappingProps.getProperty(key);
      if (pattern != null) {
        pattern = pattern.trim();
        log.debug("adding prioritized property [{}] with pattern [{}]", key, pattern);
        // note order of rule addition is critical
        digester.addObjectCreate(pattern, PrioritizedProperty.class);
        digester.addSetNext(pattern, "addPrioritizedProperty");
        digester.addRule(pattern, new SetLiteralRule("setPriority", i));
        digester.addRule(pattern, new SetLiteralRule("setName", name));
        digester.addCallMethod(pattern, "setProperty", 1);
        digester.addCallParam(pattern, 0);
      }
    }
  }
}
