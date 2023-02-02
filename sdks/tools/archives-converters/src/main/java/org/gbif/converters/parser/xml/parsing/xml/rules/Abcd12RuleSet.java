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

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.digester.Digester;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
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

    // abcd simple fields
    addNonNullMethod(digester, "catalogueNumber", "setCatalogueNumber", 1);
    addNonNullParam(digester, "catalogueNumber", 0);

    addNonNullMethod(digester, "altitudePrecision", "setAltitudePrecision", 1);
    addNonNullParam(digester, "altitudePrecision", 0);

    addNonNullMethod(digester, "depthPrecision", "setDepthPrecision", 1);
    addNonNullParam(digester, "depthPrecision", 0);

    addNonNullMethod(digester, "locality", "setLocality", 1);
    addNonNullParam(digester, "locality", 0);

    addNonNullMethod(digester, "latitude", "setLatitude", 1);
    addNonNullParam(digester, "latitude", 0);

    addNonNullMethod(digester, "longitude", "setLongitude", 1);
    addNonNullParam(digester, "longitude", 0);

    addNonNullPrioritizedProperty(digester, "country", PrioritizedPropertyNameEnum.COUNTRY, 4);
    addNonNullPrioritizedProperty(
        digester, "collectorName", PrioritizedPropertyNameEnum.COLLECTOR_NAME, 3);
    addNonNullPrioritizedProperty(
        digester, "dateCollected", PrioritizedPropertyNameEnum.DATE_COLLECTED, 3);

    // possibly many identifications
    String pattern = mappingProps.getProperty("idElement");
    if (pattern != null) {
      pattern = pattern.trim();
      digester.addObjectCreate(pattern, Identification.class);
      digester.addSetNext(pattern, "addIdentification");

      addNonNullMethod(digester, "idPreferredElement", "setPreferredAsString", 1);
      addNonNullAttParam(digester, "idPreferredElement", "idPreferredAttribute", 0);

      addNonNullMethod(digester, "idGenus", "setGenus", 1);
      addNonNullParam(digester, "idGenus", 0);

      addNonNullMethod(digester, "idGenus", "setGenus", 1);
      addNonNullParam(digester, "idGenus", 0);

      addNonNullMethod(digester, "idIdentifierName", "setIdentifierName", 1);
      addNonNullParam(digester, "idIdentifierName", 0);

      addNonNullPrioritizedProperty(
          digester, "idDateIdentified", PrioritizedPropertyNameEnum.ID_DATE_IDENTIFIED, 3);
      addNonNullPrioritizedProperty(
          digester, "idScientificName", PrioritizedPropertyNameEnum.ID_SCIENTIFIC_NAME, 2);

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

      addNonNullMethod(digester, "imageType", "setRawImageType", 1);
      addNonNullParam(digester, "imageType", 0);

      addNonNullMethod(digester, "imageDescription", "setDescription", 1);
      addNonNullParam(digester, "imageDescription", 0);

      addNonNullMethod(digester, "imageRights", "setRights", 1);
      addNonNullParam(digester, "imageRights", 0);

      addNonNullMethod(digester, "imageUrl", "setUrl", 1);
      addNonNullParam(digester, "imageUrl", 0);
    }

    // NOTE: no links in abcd 1.2
  }
}
