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
package org.gbif.pipelines.transform;

import java.io.Serializable;
import java.time.Instant;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.common.parsers.OccurrenceStatusParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.core.*;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transform.utils.VocabularyServiceFactory;

/** */
public class BasicTransform implements Serializable {

  private final PipelinesConfig config;

  private BasicTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static BasicTransform create(PipelinesConfig config) {
    return new BasicTransform(config);
  }

  public BasicRecord convert(ExtendedRecord source) {
    if (source == null || source.getCoreTerms().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord is null or empty");
    }

    BasicRecord record =
        BasicRecord.newBuilder()
            .setId(source.getId())
            .setCoreId(source.getCoreId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    var vocabServiceUrl = config.getVocabularyService().getWsUrl();
    var vocabService = VocabularyServiceFactory.getInstance(config);

    // Apply interpreters sequentially
    BasicInterpreter.interpretBasisOfRecord(source, record);
    BasicInterpreter.interpretTypifiedName(source, record);
    VocabularyInterpreter.interpretSex(vocabService).accept(source, record);
    VocabularyInterpreter.interpretTypeStatus(vocabService).accept(source, record);
    BasicInterpreter.interpretIndividualCount(source, record);
    CoreInterpreter.interpretReferences(source, record, record::setReferences);
    BasicInterpreter.interpretOrganismQuantity(source, record);
    BasicInterpreter.interpretOrganismQuantityType(source, record);
    CoreInterpreter.interpretSampleSizeUnit(source, record::setSampleSizeUnit);
    CoreInterpreter.interpretSampleSizeValue(source, record::setSampleSizeValue);
    BasicInterpreter.interpretRelativeOrganismQuantity(record);
    CoreInterpreter.interpretLicense(source, record::setLicense);
    BasicInterpreter.interpretIdentifiedByIds(source, record);
    BasicInterpreter.interpretRecordedByIds(source, record);
    // FIXME
    BasicInterpreter.interpretOccurrenceStatus(OccurrenceStatusParserKvStore.create())
        .accept(source, record);
    VocabularyInterpreter.interpretEstablishmentMeans(vocabService).accept(source, record);
    VocabularyInterpreter.interpretLifeStage(vocabService).accept(source, record);
    VocabularyInterpreter.interpretPathway(vocabService).accept(source, record);
    VocabularyInterpreter.interpretDegreeOfEstablishment(vocabService).accept(source, record);
    CoreInterpreter.interpretDatasetID(source, record::setDatasetID);
    CoreInterpreter.interpretDatasetName(source, record::setDatasetName);
    BasicInterpreter.interpretOtherCatalogNumbers(source, record);
    BasicInterpreter.interpretRecordedBy(source, record);
    BasicInterpreter.interpretIdentifiedBy(source, record);
    BasicInterpreter.interpretPreparations(source, record);
    CoreInterpreter.interpretSamplingProtocol(source, record::setSamplingProtocol);
    BasicInterpreter.interpretProjectId(source, record);
    BasicInterpreter.interpretIsSequenced(source, record);
    BasicInterpreter.interpretAssociatedSequences(source, record);

    // Geological context
    GeologicalContextInterpreter.interpretChronostratigraphy(vocabService).accept(source, record);
    GeologicalContextInterpreter.interpretLowestBiostratigraphicZone(source, record);
    GeologicalContextInterpreter.interpretHighestBiostratigraphicZone(source, record);
    GeologicalContextInterpreter.interpretGroup(source, record);
    GeologicalContextInterpreter.interpretFormation(source, record);
    GeologicalContextInterpreter.interpretMember(source, record);
    GeologicalContextInterpreter.interpretBed(source, record);

    // Dynamic properties
    DynamicPropertiesInterpreter.interpretSex(vocabService).accept(source, record);
    DynamicPropertiesInterpreter.interpretLifeStage(vocabService).accept(source, record);

    return record;
  }

  // This versions will be replaced by vocabulary server in the future
  @NoArgsConstructor(staticName = "create")
  public static class OccurrenceStatusParserKvStore
      implements KeyValueStore<String, OccurrenceStatus>, Serializable {

    private final OccurrenceStatusParser parser = OccurrenceStatusParser.getInstance();

    @Override
    public OccurrenceStatus get(String s) {
      ParseResult<OccurrenceStatus> parse = parser.parse(s);
      return parse.isSuccessful() ? parse.getPayload() : null;
    }

    @Override
    public void close() {
      // NOP
    }
  }
}
