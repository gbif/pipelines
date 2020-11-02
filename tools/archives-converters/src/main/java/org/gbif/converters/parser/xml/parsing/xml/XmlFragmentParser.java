/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.converters.parser.xml.parsing.xml;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.RuleSet;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.identifier.OccurrenceKeyHelper;
import org.gbif.converters.parser.xml.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.converters.parser.xml.identifier.Triplet;
import org.gbif.converters.parser.xml.identifier.UniqueIdentifier;
import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.gbif.converters.parser.xml.parsing.xml.rules.Abcd12RuleSet;
import org.gbif.converters.parser.xml.parsing.xml.rules.Abcd206RuleSet;
import org.gbif.converters.parser.xml.parsing.xml.rules.Dwc10RuleSet;
import org.gbif.converters.parser.xml.parsing.xml.rules.Dwc14RuleSet;
import org.gbif.converters.parser.xml.parsing.xml.rules.Dwc2009RuleSet;
import org.gbif.converters.parser.xml.parsing.xml.rules.DwcManisRuleSet;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Methods for parsing {@link RawXmlOccurrence}s and {@link UniqueIdentifier}s from xml fragments.
 */
@Slf4j
public class XmlFragmentParser {

  private static final Map<OccurrenceSchemaType, RuleSet> RULE_SETS = Maps.newHashMap();

  // static class, should never be instantiated
  private XmlFragmentParser() {}

  static {
    try {
      RULE_SETS.put(OccurrenceSchemaType.ABCD_1_2, new Abcd12RuleSet());
      RULE_SETS.put(OccurrenceSchemaType.ABCD_2_0_6, new Abcd206RuleSet());
      RULE_SETS.put(OccurrenceSchemaType.DWC_1_0, new Dwc10RuleSet());
      RULE_SETS.put(OccurrenceSchemaType.DWC_1_4, new Dwc14RuleSet());
      RULE_SETS.put(OccurrenceSchemaType.DWC_2009, new Dwc2009RuleSet());
      RULE_SETS.put(OccurrenceSchemaType.DWC_MANIS, new DwcManisRuleSet());
    } catch (IOException e) {
      log.warn("Unable to read properties files for parsing xml", e);
    }
  }

  public static List<RawOccurrenceRecord> parseRecord(RawXmlOccurrence xmlRecord) {
    return parseRecord(xmlRecord.getXml(), xmlRecord.getSchemaType());
  }

  public static List<RawOccurrenceRecord> parseRecord(String xml, OccurrenceSchemaType schemaType) {
    log.debug("Parsing xml [{}]", xml);
    List<RawOccurrenceRecord> records = null;
    try {
      InputSource inputSource = new InputSource(new StringReader(xml));
      records = parseRecord(inputSource, schemaType);
    } catch (IOException e) {
      log.warn("IOException parsing xml string [{}]", xml, e);
    } catch (SAXException e) {
      log.warn("SAXException parsing xml string [{}]", xml, e);
    }
    return forceIdentifiers(records);
  }

  public static List<RawOccurrenceRecord> parseRecord(byte[] xml, OccurrenceSchemaType schemaType) {
    List<RawOccurrenceRecord> records = null;
    try {
      InputSource inputSource = new InputSource(new ByteArrayInputStream(xml));
      records = parseRecord(inputSource, schemaType);
    } catch (IOException e) {
      log.warn("IOException parsing xml bytes", e);
    } catch (SAXException e) {
      log.warn("SAXException parsing xml bytes", e);
    }
    return records;
  }

  private static List<RawOccurrenceRecord> parseRecord(
      InputSource inputSource, OccurrenceSchemaType schemaType) throws IOException, SAXException {
    RawOccurrenceRecordBuilder builder = new RawOccurrenceRecordBuilder();
    Digester digester = new Digester();
    digester.setNamespaceAware(true);
    digester.setValidating(false);
    digester.push(builder);
    digester.addRuleSet(RULE_SETS.get(schemaType));
    digester.parse(inputSource);

    builder.resolvePriorities();
    return builder.generateRawOccurrenceRecords();
  }

  /**
   * This method is a hack to return a single result where ScientificName matches the given
   * unitQualifier. This behaviour is only relevant for ABCD 2.06 - the others all produce a single
   * record anyway. TODO: refactor the parse/builder to return what we want, rather than hacking
   * around FIXME: never used, can it be deleted?? if it is gonna be used we will have to set the
   * missing ids
   */
  public static RawOccurrenceRecord parseRecord(
      byte[] xml, OccurrenceSchemaType schemaType, String unitQualifier) {
    RawOccurrenceRecord result = null;
    List<RawOccurrenceRecord> records = parseRecord(xml, schemaType);
    if (records == null || records.isEmpty()) {
      log.warn("Could not parse any records from given xml - returning null.");
    } else if (records.size() == 1) {
      result = records.get(0);
    } else if (unitQualifier == null) {
      log.warn(
          "Got multiple records from given xml, but no unitQualifier set - returning first record as a guess.");
      result = records.get(0);
    } else {
      for (RawOccurrenceRecord record : records) {
        if (record.getScientificName().equals(unitQualifier)) {
          result = record;
          break;
        }
      }
      if (result == null) {
        log.warn("Got multiple records from xml but none matched unitQualifier - returning null");
      }
    }

    return result;
  }

  /**
   * Extract sets of UniqueIdentifiers from the xml snippet. In the usual case the set will contain
   * a single result, which will in turn contain 1 or more UniqueIdentifiers for the given xml. In
   * the ABCD 2 case there may be more than one occurrence represented by the given xml, in which
   * case there will be an IdentifierExtractionResult (with UniqueIdentifiers) returned for each of
   * the represented occurrences (e.g. if 3 occurrences are in the xml snippet and each have one
   * UniqueIdentifier the result will be a set of 3 IdentifierExtractionResults, where each result
   * contains a single UniqueIdentifier). If the passed in xml is somehow malformed there may be 0
   * UniqueIdentifiers found, in which case an empty set is returned.
   *
   * @param datasetKey UUID for this dataset
   * @param xml snippet of xml representing one (or more, in ABCD) occurrence
   * @param schemaType the protocol that produced this xml (e.g. DWC, ABCD)
   * @param useOccurrenceId @return a set of 0 or more IdentifierExtractionResults containing
   *     UniqueIdentifiers as found in the xml
   * @see UniqueIdentifier
   */
  public static Set<IdentifierExtractionResult> extractIdentifiers(
      UUID datasetKey,
      byte[] xml,
      OccurrenceSchemaType schemaType,
      boolean useTriplet,
      boolean useOccurrenceId) {
    Set<IdentifierExtractionResult> results = Sets.newHashSet();

    // this is somewhat wasteful, but a whole separate stack of parsing to extract triplet seems
    // excessive
    List<RawOccurrenceRecord> records = parseRecord(xml, schemaType);
    if (records != null && !records.isEmpty()) {
      for (RawOccurrenceRecord record : records) {
        Set<UniqueIdentifier> ids = Sets.newHashSet();

        if (useTriplet) {
          Triplet triplet = null;
          try {
            triplet =
                new Triplet(
                    datasetKey,
                    record.getInstitutionCode(),
                    record.getCollectionCode(),
                    record.getCatalogueNumber(),
                    record.getUnitQualifier());
          } catch (IllegalArgumentException e) {
            // some of the triplet was null or empty, so it's not valid - that's highly suspicious,
            // but could be ok...
            log.info(
                "No holy triplet for an xml snippet in dataset [{}] and schema [{}], got error [{}]",
                datasetKey.toString(),
                schemaType.toString(),
                e.getMessage());
          }
          if (triplet != null) {
            ids.add(triplet);
          }
        }

        if (useOccurrenceId
            && record.getIdentifierRecords() != null
            && !record.getIdentifierRecords().isEmpty()) {
          for (IdentifierRecord idRecord : record.getIdentifierRecords()) {
            // TODO: this needs much better checking (ie can we trust that guid (type 1) and
            // sourceid (type 7) are
            // getting set and parsed properly?)
            // TODO: identifier types need to be enums
            if ((idRecord.getIdentifierType() == 1 || idRecord.getIdentifierType() == 7)
                && idRecord.getIdentifier() != null) {
              ids.add(new PublisherProvidedUniqueIdentifier(datasetKey, idRecord.getIdentifier()));
            }
          }
        }

        if (!ids.isEmpty()) {
          results.add(new IdentifierExtractionResult(ids, record.getUnitQualifier()));
        }
      }
    }

    return results;
  }

  /** Filters the records by discarding the ones without ID. */
  private static List<RawOccurrenceRecord> forceIdentifiers(List<RawOccurrenceRecord> records) {
    if (records == null) {
      return Collections.emptyList();
    }

    List<RawOccurrenceRecord> result = new ArrayList<>(records.size());
    for (RawOccurrenceRecord record : records) {
      // try to set an id in case it is missing
      if (Strings.isNullOrEmpty(record.getId())) {
        // try to create a triplet
        try {
          record.setId(
              OccurrenceKeyHelper.toKey(
                  new Triplet(
                      record.getInstitutionCode(),
                      record.getCollectionCode(),
                      record.getCatalogueNumber())));
        } catch (Exception e) {
          log.info(
              "Could not create a triplet for record with institution code {}, collection code {} and catalogue "
                  + "number {}",
              record.getInstitutionCode(),
              record.getCollectionCode(),
              record.getCatalogueNumber(),
              e);
          // record discarded
          continue;
        }
      }
      result.add(record);
    }
    return result;
  }
}
