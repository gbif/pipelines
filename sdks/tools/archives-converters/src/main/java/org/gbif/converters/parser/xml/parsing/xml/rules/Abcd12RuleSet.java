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
package org.gbif.converters.parser.xml.parsing.xml.rules;

import static org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum.*;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.commons.digester.Digester;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.model.Collector;
import org.gbif.converters.parser.xml.model.Identification;
import org.gbif.converters.parser.xml.model.ImageRecord;

public class Abcd12RuleSet extends AbstractRuleSet {

  private static final String MAPPING_FILE = "mapping/indexMapping_abcd_1_2.properties";

  public Abcd12RuleSet() throws IOException {
    mappingProps = new Properties();
    URL url = ClassLoader.getSystemResource(MAPPING_FILE);
    mappingProps.load(url.openStream());
  }

  @Override
  public String getNamespaceURI() {
    return OccurrenceSchemaType.ABCD_1_2.toString();
  }

  @Override
  public void addRuleInstances(Digester digester) {
    super.addRuleInstances(digester);

    BiConsumer<String, String> addFn =
        (property, methodName) -> {
          addNonNullMethod(digester, property, methodName, 1);
          addNonNullParam(digester, property, 0);
        };

    // abcd simple fields
    addFn.accept("catalogueNumber", "setCatalogueNumber");
    addFn.accept("altitudePrecision", "setAltitudePrecision");
    addFn.accept("depthPrecision", "setDepthPrecision");
    addFn.accept("locality", "setLocality");
    addFn.accept("latitudeDecimal", "setDecimalLatitude");
    addFn.accept("longitudeDecimal", "setDecimalLongitude");

    addNonNullPrioritizedProperty(digester, "country", COUNTRY, 2);
    addNonNullPrioritizedProperty(digester, "countryCode", COUNTRY_CODE, 2);
    addNonNullPrioritizedProperty(digester, "dateCollected", DATE_COLLECTED, 3);

    // possibly many identifications
    String pattern = mappingProps.getProperty("idElement");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, Identification.class);
      digester.addSetNext(pattern, "addIdentification");

      addNonNullMethod(digester, "idPreferredElement", "setPreferredAsString", 1);
      addNonNullAttParam(digester, "idPreferredElement", "idPreferredAttribute", 0);

      addFn.accept("idGenus", "setGenus");
      addFn.accept("idIdentifierName", "setIdentifierName");

      addNonNullPrioritizedProperty(digester, "idDateIdentified", ID_DATE_IDENTIFIED, 3);
      addNonNullPrioritizedProperty(digester, "idScientificName", ID_SCIENTIFIC_NAME, 2);

      // possibly many higher taxons for every identification
      addNonNullMethod(digester, "higherTaxonElement", "addHigherTaxon", 2);
      addNonNullParam(digester, "higherTaxonRank", 0);
      addNonNullAttParam(digester, "higherTaxonRankElement", "higherTaxonRankAttribute", 0);
      addNonNullParam(digester, "higherTaxonName", 1);
    }

    // possibly many images
    pattern = mappingProps.getProperty("imageElement");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, ImageRecord.class);
      digester.addSetNext(pattern, "addImage");

      addFn.accept("imageType", "setRawImageType");
      addFn.accept("imageDescription", "setDescription");
      addFn.accept("imageRights", "setRights");
      addFn.accept("imageUrl", "setUrl");
    }

    // NOTE: no links in abcd 1.2 possible many collectors
    Consumer<String> collectorFn =
        prop -> {
          String ptrn = mappingProps.getProperty(prop);
          if (ptrn != null) {
            ptrn = ptrn.trim();
            digester.addObjectCreate(ptrn, Collector.class);
            digester.addSetNext(ptrn, "addCollectorName");

            addFn.accept(prop, "setName");
          }
        };

    collectorFn.accept("collectorNameFullName");
    collectorFn.accept("collectorNameGatheringAgentsText");
    collectorFn.accept("collectorNameAgentText");
  }
}
