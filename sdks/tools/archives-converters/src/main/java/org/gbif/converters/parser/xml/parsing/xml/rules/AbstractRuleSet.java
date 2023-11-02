package org.gbif.converters.parser.xml.parsing.xml.rules;

import java.util.Properties;
import java.util.function.BiConsumer;
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

    BiConsumer<String, String> addFn =
        (property, methodName) -> {
          addNonNullMethod(digester, property, methodName, 1);
          addNonNullParam(digester, property, 0);
        };

    // common, simple fields
    addFn.accept("institutionCode", "setInstitutionCode");
    addFn.accept("collectionCode", "setCollectionCode");
    addFn.accept("basisOfRecord", "setBasisOfRecord");
    addFn.accept("minDepth", "setMinDepth");
    addFn.accept("maxDepth", "setMaxDepth");
    addFn.accept("latLongPrecision", "setLatLongPrecision");
    addFn.accept("minAltitude", "setMinAltitude");
    addFn.accept("maxAltitude", "setMaxAltitude");

    // identifier records
    addFn.accept("identifierType1", "setIdentifierType1");
    addFn.accept("identifierType2", "setIdentifierType2");
    addFn.accept("identifierType3", "setIdentifierType3");
    addFn.accept("identifierType4", "setIdentifierType4");
    addFn.accept("identifierType5", "setIdentifierType5");
    addFn.accept("identifierType6", "setIdentifierType6");
    addFn.accept("identifierType7", "setIdentifierType7");

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
