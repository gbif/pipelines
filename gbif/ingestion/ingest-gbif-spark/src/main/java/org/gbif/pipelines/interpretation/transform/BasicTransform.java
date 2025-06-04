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
package org.gbif.pipelines.interpretation.transform;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import org.gbif.pipelines.core.interpreters.*;
import org.gbif.pipelines.core.interpreters.core.*;
import org.gbif.pipelines.interpretation.transform.utils.VocabularyServiceFactory;
import org.gbif.pipelines.io.avro.*;

/** */
@Builder
public class BasicTransform implements Serializable {
  private boolean useDynamicPropertiesInterpretation;
  private String vocabularyApiUrl;

  public Optional<BasicRecord> convert(ExtendedRecord source) {

    Interpretation<ExtendedRecord>.Handler<BasicRecord> handler =
        Interpretation.from(source)
            .to(
                BasicRecord.newBuilder()
                    .setId(source.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
            .when(er -> !er.getCoreTerms().isEmpty())
            .via(BasicInterpreter::interpretBasisOfRecord)
            .via(BasicInterpreter::interpretTypifiedName)
            .via(
                VocabularyInterpreter.interpretSex(
                    VocabularyServiceFactory.getInstance(vocabularyApiUrl)))
            .via(
                VocabularyInterpreter.interpretTypeStatus(
                    VocabularyServiceFactory.getInstance(vocabularyApiUrl)))
            .via(BasicInterpreter::interpretIndividualCount)
            .via((e, r) -> CoreInterpreter.interpretReferences(e, r, r::setReferences))
            .via(BasicInterpreter::interpretOrganismQuantity)
            .via(BasicInterpreter::interpretOrganismQuantityType)
            .via((e, r) -> CoreInterpreter.interpretSampleSizeUnit(e, r::setSampleSizeUnit))
            .via((e, r) -> CoreInterpreter.interpretSampleSizeValue(e, r::setSampleSizeValue))
            .via(BasicInterpreter::interpretRelativeOrganismQuantity)
            .via((e, r) -> CoreInterpreter.interpretLicense(e, r::setLicense))
            .via(BasicInterpreter::interpretIdentifiedByIds)
            .via(BasicInterpreter::interpretRecordedByIds)
            /*
            .via(
                VocabularyInterpreter.interpretOccurrenceStatus(
                    VocabularyServiceFactory.getInstance(vocabularyApiUrl)))

             */
            .via(
                VocabularyInterpreter.interpretEstablishmentMeans(
                    VocabularyServiceFactory.getInstance(vocabularyApiUrl)))
            .via(
                VocabularyInterpreter.interpretLifeStage(
                    VocabularyServiceFactory.getInstance(vocabularyApiUrl)))
            .via(
                VocabularyInterpreter.interpretPathway(
                    VocabularyServiceFactory.getInstance(vocabularyApiUrl)))
            .via(
                VocabularyInterpreter.interpretDegreeOfEstablishment(
                    VocabularyServiceFactory.getInstance(vocabularyApiUrl)))
            .via((e, r) -> CoreInterpreter.interpretDatasetID(e, r::setDatasetID))
            .via((e, r) -> CoreInterpreter.interpretDatasetName(e, r::setDatasetName))
            .via(BasicInterpreter::interpretOtherCatalogNumbers)
            .via(BasicInterpreter::interpretRecordedBy)
            .via(BasicInterpreter::interpretIdentifiedBy)
            .via(BasicInterpreter::interpretPreparations)
            .via((e, r) -> CoreInterpreter.interpretSamplingProtocol(e, r::setSamplingProtocol))
            .via(BasicInterpreter::interpretProjectId)
            .via(BasicInterpreter::interpretIsSequenced)
            .via(BasicInterpreter::interpretAssociatedSequences)
            // Geological context
            .via(
                GeologicalContextInterpreter.interpretChronostratigraphy(
                    VocabularyServiceFactory.getInstance(vocabularyApiUrl)))
            .via(GeologicalContextInterpreter::interpretLowestBiostratigraphicZone)
            .via(GeologicalContextInterpreter::interpretHighestBiostratigraphicZone)
            .via(GeologicalContextInterpreter::interpretGroup)
            .via(GeologicalContextInterpreter::interpretFormation)
            .via(GeologicalContextInterpreter::interpretMember)
            .via(GeologicalContextInterpreter::interpretBed);

    if (useDynamicPropertiesInterpretation) {
      handler
          .via(
              DynamicPropertiesInterpreter.interpretSex(
                  VocabularyServiceFactory.getInstance(vocabularyApiUrl)))
          .via(
              DynamicPropertiesInterpreter.interpretLifeStage(
                  VocabularyServiceFactory.getInstance(vocabularyApiUrl)));
    }

    return handler.getOfNullable();
  }
}
