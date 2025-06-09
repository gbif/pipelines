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
package org.gbif.pipelines.interpretation.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.gbif.pipelines.interpretation.transform.TemporalTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

public class TemporalInterpretation {

  /** Interprets the temporal information contained in the extended records. */
  public static Dataset<TemporalRecord> temporalTransform(Dataset<ExtendedRecord> source) {

    TemporalTransform temporalTransform =
        TemporalTransform.builder().build(); // TODO: add orderings from dataset tags

    return source.map(
        (MapFunction<ExtendedRecord, TemporalRecord>)
            er -> temporalTransform.convert(er).orElse(null),
        Encoders.bean(TemporalRecord.class));
  }
}
