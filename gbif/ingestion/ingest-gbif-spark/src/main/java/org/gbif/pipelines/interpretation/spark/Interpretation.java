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

import static org.gbif.pipelines.interpretation.spark.LocationInterpretation.locationTransform;
import static org.gbif.pipelines.interpretation.spark.TaxonomyInterpretation.taxonomyTransform;
import static org.gbif.pipelines.interpretation.spark.TemporalInterpretation.temporalTransform;

import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.interpretation.transform.BasicTransform;
import org.gbif.pipelines.io.avro.*;

public class Interpretation implements Serializable {
  public static void main(String[] args) throws IOException {
    Config config = Config.fromFirstArg(args);

    SparkSession.Builder sb = SparkSession.builder();

    if (config.getSparkRemote() != null) sb.remote(config.getSparkRemote());
    SparkSession spark = sb.getOrCreate();
    if (config.getJarPath() != null) spark.addArtifact(config.getJarPath());

    // Read the verbatim input
    Dataset<ExtendedRecord> records =
        spark.read().format("avro").load(config.getInput()).as(Encoders.bean(ExtendedRecord.class));

    // Run the interpretations
    Dataset<BasicRecord> basic = basicTransform(config, records);
    Dataset<LocationRecord> location = locationTransform(config, spark, records);
    Dataset<TemporalRecord> temporal = temporalTransform(records);
    Dataset<MultiTaxonRecord> taxonomy = taxonomyTransform(config, spark, records);

    // Write the intermediate output (useful for debugging)
    basic.write().mode("overwrite").parquet(config.getOutput() + "/basic");
    location.write().mode("overwrite").parquet(config.getOutput() + "/location");
    temporal.write().mode("overwrite").parquet(config.getOutput() + "/temporal");
    taxonomy.write().mode("overwrite").parquet(config.getOutput() + "/taxonomy");

    // TODO: read and join all the intermediate outputs to the HDFS and JSON views
    spark.close();
  }

  private static Dataset<BasicRecord> basicTransform(
      Config config, Dataset<ExtendedRecord> source) {
    return source.map(
        (MapFunction<ExtendedRecord, BasicRecord>)
            er ->
                BasicTransform.builder()
                    .useDynamicPropertiesInterpretation(true)
                    .vocabularyApiUrl(config.getVocabularyApiUrl())
                    .build()
                    .convert(er)
                    .get(),
        Encoders.bean(BasicRecord.class));
  }
}
