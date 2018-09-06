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

import org.gbif.converters.parser.xml.constants.TaxonRankEnum;
import org.gbif.converters.parser.xml.model.Taxon;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HigherTaxonParser {

  private static final Logger LOG = LoggerFactory.getLogger(HigherTaxonParser.class);

  private final Properties taxonRankMapping = new Properties();
  private final String taxonRankMappingFilename = "/taxonRankMapping.properties";

  public HigherTaxonParser() {
    init();
  }

  private void init() {
    // load taxonRank mappings
    try {
      InputStream is = getClass().getResourceAsStream(taxonRankMappingFilename);
      taxonRankMapping.load(is);
    } catch (IOException e) {
      LOG.error("Unable to load taxonRankMapping - parsing higher taxons will fail", e);
    }
  }

  /**
   * Given a raw taxon rank and name, finds the matching Linnean rank (if any) and builds a Taxon
   * object with the rank and name.
   */
  public Taxon parseTaxon(String rawTaxonRank, String taxonName) {
    Taxon taxon = null;
    String processedTaxonRank = Strings.emptyToNull(rawTaxonRank);
    if (processedTaxonRank != null && !"null".equals(processedTaxonRank)) {
      processedTaxonRank = processedTaxonRank.replaceAll(" ", "").toUpperCase();
      String rawRank = taxonRankMapping.getProperty(processedTaxonRank);
      if (rawRank == null) {
        LOG.info("Could not process taxon ranking of [{}], skipping.", processedTaxonRank);
      } else {
        int rank = Integer.parseInt(rawRank.trim());
        LOG.debug("ProcessedTaxonRank [{}] gives numeric rank [{}]", processedTaxonRank, rank);
        switch (rank) {
          case 1000:
            taxon = new Taxon(TaxonRankEnum.KINGDOM, taxonName);
            break;
          case 2000:
            taxon = new Taxon(TaxonRankEnum.PHYLUM, taxonName);
            break;
          case 3000:
            taxon = new Taxon(TaxonRankEnum.CLASS, taxonName);
            break;
          case 4000:
            taxon = new Taxon(TaxonRankEnum.ORDER, taxonName);
            break;
          case 5000:
            taxon = new Taxon(TaxonRankEnum.FAMILY, taxonName);
            break;
        }
      }
    }

    return taxon;
  }
}
