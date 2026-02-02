/*
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
package org.gbif.pipelines.transform.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.vocabulary.lookup.InMemoryVocabularyLookup;

/** Provides the {@link VocabularyService} as a singleton per JVM. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VocabularyServiceFactory {
  private static final Object LOCK = new Object();
  private static VocabularyService vocabularyService;

  /**
   * Creates or retrieves the singleton instance of the VocabularyService.
   *
   * @param config pipelines config
   * @return the singleton VocabularyService instance
   */
  public static VocabularyService getInstance(PipelinesConfig config) {

    if (vocabularyService == null) {
      synchronized (LOCK) {
        if (vocabularyService == null) {

          if (config != null
              && config.getVocabularyService() != null
              && config.getVocabularyService().getWsUrl() != null) {

            String url = config.getVocabularyService().getWsUrl();
            vocabularyService =
                VocabularyService.builder()
                    .vocabularyLookup(
                        DwcTerm.lifeStage.qualifiedName(),
                        InMemoryVocabularyLookup.newBuilder().from(url, "LifeStage").build())
                    .vocabularyLookup(
                        DwcTerm.degreeOfEstablishment.qualifiedName(),
                        InMemoryVocabularyLookup.newBuilder()
                            .from(url, "DegreeOfEstablishment")
                            .build())
                    .vocabularyLookup(
                        DwcTerm.establishmentMeans.qualifiedName(),
                        InMemoryVocabularyLookup.newBuilder()
                            .from(url, "EstablishmentMeans")
                            .build())
                    .vocabularyLookup(
                        DwcTerm.pathway.qualifiedName(),
                        InMemoryVocabularyLookup.newBuilder().from(url, "Pathway").build())
                    .vocabularyLookup(
                        DwcTerm.typeStatus.qualifiedName(),
                        InMemoryVocabularyLookup.newBuilder().from(url, "TypeStatus").build())
                    .vocabularyLookup(
                        DwcTerm.sex.qualifiedName(),
                        InMemoryVocabularyLookup.newBuilder().from(url, "Sex").build())
                    .build();
          } else if (config != null
              && config.getVocabularyConfig() != null
              && config.getVocabularyConfig().getVocabulariesPath() != null) {
            HdfsConfigs hdfsConfigs =
                HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());
            vocabularyService = FileVocabularyFactory.getInstance(hdfsConfigs, config);

          } else {
            throw new IllegalArgumentException("VocabularyService configuration is missing");
          }
        }
      }
    }

    return vocabularyService;
  }
}
