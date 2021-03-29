package org.gbif.pipelines.clustering;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.spark.sql.*;
import org.junit.Test;

/**
 * Unit tests for OccurrenceFeatures.
 *
 * <p>These use a parquet file as input created using the following (CC0 data):
 *
 * <pre>
 *   CREATE TABLE tim.test STORED AS parquet AS
 *   SELECT * FROM prod_h.occurrence
 *   WHERE datasetKey='50c9509d-22c7-4a22-a47d-8c48425ef4a7' AND recordedBy='Tim Robertson'
 * </pre>
 */
public class OccurrenceFeaturesSparkTest extends BaseSparkTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Test to ensure that JSON is generated correctly for nested JSON. */
  @Test
  public void testAsJsonWithMultimedia() throws IOException {
    Dataset<Row> data = sqlContext.read().parquet("src/test/resources/sample.parquet");
    Row first = data.first();

    // read and format the JSON from the first row
    String multimedia = first.getString(first.fieldIndex("ext_multimedia"));
    String formattedMultimedia =
        OBJECT_MAPPER.writeValueAsString(OBJECT_MAPPER.readTree(multimedia));

    RowOccurrenceFeatures features = new RowOccurrenceFeatures(first, null, "ext_multimedia");

    // ensure that the resulting JSON is not escaped
    assertTrue(features.asJson().contains(formattedMultimedia));
  }
}
