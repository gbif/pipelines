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
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.core.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/**
 * Beam level transformations for the DWC Event, reads an avro, writes an avro, maps from value to
 * keyValue and transforms form {@link ExtendedRecord} to {@link TemporalRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link TemporalRecord} using {@link ExtendedRecord}
 * as a source and {@link TemporalInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#event">https://dwc.tdwg.org/terms/#event</a>
 */
@Slf4j
public class TemporalTransform implements Serializable {

  private final TemporalInterpreter temporalInterpreter;

  private TemporalTransform(TemporalInterpreter temporalInterpreter) {
    this.temporalInterpreter = temporalInterpreter;
  }

  public static TemporalTransform create(PipelinesConfig config) {
    return new TemporalTransform(
        TemporalInterpreter.builder().orderings(config.getDefaultDateFormat()).create());
  }

  public TemporalRecord convert(ExtendedRecord source) {

    if (source == null || source.getCoreTerms().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord is null or empty");
    }

    TemporalRecord record =
        TemporalRecord.newBuilder()
            .setId(source.getId())
            .setCoreId(source.getCoreId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    // Sequentially apply interpreters
    temporalInterpreter.interpretTemporal(source, record);
    temporalInterpreter.interpretModified(source, record);
    temporalInterpreter.interpretDateIdentified(source, record);
    TemporalInterpreter.setCoreId(source, record);
    TemporalInterpreter.setParentEventId(source, record);

    return record;
  }
}
