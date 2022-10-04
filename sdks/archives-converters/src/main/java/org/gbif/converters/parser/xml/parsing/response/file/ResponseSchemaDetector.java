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
package org.gbif.converters.parser.xml.parsing.response.file;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.constants.ExtractionSimpleXPaths;
import org.gbif.converters.parser.xml.constants.ResponseElementEnum;

/**
 * Primary purpose of this class is, given a raw occurrence record serialized to String from XML,
 * determine which OccurrenceSchemaType it matches.
 */
@Slf4j
public class ResponseSchemaDetector {

  // a dumb way to guarantee that string searching will be
  // done such that eg "CatalogNumberText" gets searched for before "CatalogNumber"
  private List<OccurrenceSchemaType> schemaSearchOrder;
  private Map<OccurrenceSchemaType, Map<ResponseElementEnum, String>> distinctiveElements;

  public ResponseSchemaDetector() {
    init();
  }

  private boolean checkElements(String xml, Collection<String> elements) {
    boolean result;
    for (String elem : elements) {
      result = xml.contains(elem);
      log.debug("Xml contains [{}] is [{}]", elem, result);
      if (!result) {
        return false;
      }
    }

    return true;
  }

  public OccurrenceSchemaType detectSchema(String xml) {
    OccurrenceSchemaType result = null;
    for (OccurrenceSchemaType schema : schemaSearchOrder) {
      log.debug("Checking for schema [{}]", schema);
      boolean success = checkElements(xml, distinctiveElements.get(schema).values());
      if (success) {
        result = schema;
        break;
      }
    }

    if (result == null) {
      log.warn("Could not determine schema for xml [{}]", xml);
    }

    return result;
  }

  public Map<ResponseElementEnum, String> getResponseElements(OccurrenceSchemaType schemaType) {
    return distinctiveElements.get(schemaType);
  }

  private void init() {
    schemaSearchOrder = new ArrayList<>();
    schemaSearchOrder.add(OccurrenceSchemaType.DWC_MANIS);
    schemaSearchOrder.add(OccurrenceSchemaType.DWC_1_0);
    schemaSearchOrder.add(OccurrenceSchemaType.DWC_1_4);
    schemaSearchOrder.add(OccurrenceSchemaType.DWC_2009);
    schemaSearchOrder.add(OccurrenceSchemaType.ABCD_1_2);
    schemaSearchOrder.add(OccurrenceSchemaType.ABCD_2_0_6);

    distinctiveElements = new EnumMap<>(OccurrenceSchemaType.class);

    Map<ResponseElementEnum, String> elements = new EnumMap<>(ResponseElementEnum.class);
    elements.put(ResponseElementEnum.CATALOG_NUMBER, ExtractionSimpleXPaths.DWC_1_0_CATALOG);
    elements.put(ResponseElementEnum.COLLECTION_CODE, ExtractionSimpleXPaths.DWC_1_0_COLLECTION);
    elements.put(ResponseElementEnum.INSTITUTION_CODE, ExtractionSimpleXPaths.DWC_1_0_INSTITUTION);
    elements.put(ResponseElementEnum.RECORD, ExtractionSimpleXPaths.DWC_1_0_RECORD);
    distinctiveElements.put(OccurrenceSchemaType.DWC_1_0, elements);

    elements = new EnumMap<>(ResponseElementEnum.class);
    elements.put(ResponseElementEnum.CATALOG_NUMBER, ExtractionSimpleXPaths.DWC_1_4_CATALOG);
    elements.put(ResponseElementEnum.COLLECTION_CODE, ExtractionSimpleXPaths.DWC_1_4_COLLECTION);
    elements.put(ResponseElementEnum.INSTITUTION_CODE, ExtractionSimpleXPaths.DWC_1_4_INSTITUTION);
    elements.put(ResponseElementEnum.RECORD, ExtractionSimpleXPaths.DWC_1_4_RECORD);
    distinctiveElements.put(OccurrenceSchemaType.DWC_1_4, elements);

    elements = new EnumMap<>(ResponseElementEnum.class);
    elements.put(ResponseElementEnum.CATALOG_NUMBER, ExtractionSimpleXPaths.DWC_MANIS_CATALOG);
    elements.put(ResponseElementEnum.COLLECTION_CODE, ExtractionSimpleXPaths.DWC_MANIS_COLLECTION);
    elements.put(
        ResponseElementEnum.INSTITUTION_CODE, ExtractionSimpleXPaths.DWC_MANIS_INSTITUTION);
    elements.put(ResponseElementEnum.RECORD, ExtractionSimpleXPaths.DWC_MANIS_RECORD);
    distinctiveElements.put(OccurrenceSchemaType.DWC_MANIS, elements);

    elements = new EnumMap<>(ResponseElementEnum.class);
    elements.put(ResponseElementEnum.CATALOG_NUMBER, ExtractionSimpleXPaths.DWC_2009_CATALOG);
    elements.put(ResponseElementEnum.COLLECTION_CODE, ExtractionSimpleXPaths.DWC_2009_COLLECTION);
    elements.put(ResponseElementEnum.INSTITUTION_CODE, ExtractionSimpleXPaths.DWC_2009_INSTITUTION);
    elements.put(ResponseElementEnum.RECORD, ExtractionSimpleXPaths.DWC_2009_RECORD);
    distinctiveElements.put(OccurrenceSchemaType.DWC_2009, elements);

    elements = new EnumMap<>(ResponseElementEnum.class);
    elements.put(ResponseElementEnum.CATALOG_NUMBER, ExtractionSimpleXPaths.ABCD_1_2_CATALOG);
    elements.put(ResponseElementEnum.COLLECTION_CODE, ExtractionSimpleXPaths.ABCD_1_2_COLLECTION);
    elements.put(ResponseElementEnum.INSTITUTION_CODE, ExtractionSimpleXPaths.ABCD_1_2_INSTITUTION);
    elements.put(ResponseElementEnum.RECORD, ExtractionSimpleXPaths.ABCD_1_2_RECORD);
    distinctiveElements.put(OccurrenceSchemaType.ABCD_1_2, elements);

    elements = new EnumMap<>(ResponseElementEnum.class);
    elements.put(ResponseElementEnum.CATALOG_NUMBER, ExtractionSimpleXPaths.ABCD_2_0_6_CATALOG);
    elements.put(ResponseElementEnum.COLLECTION_CODE, ExtractionSimpleXPaths.ABCD_2_0_6_COLLECTION);
    elements.put(
        ResponseElementEnum.INSTITUTION_CODE, ExtractionSimpleXPaths.ABCD_2_0_6_INSTITUTION);
    elements.put(ResponseElementEnum.RECORD, ExtractionSimpleXPaths.ABCD_2_0_6_RECORD);
    distinctiveElements.put(OccurrenceSchemaType.ABCD_2_0_6, elements);
  }
}
