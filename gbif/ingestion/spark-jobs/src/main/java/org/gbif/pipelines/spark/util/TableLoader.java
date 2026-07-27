package org.gbif.pipelines.spark.util;

import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Loads a named DwC-DP table as a Spark Dataset.
 *
 * <p>Returns {@link Optional#empty()} when the table is not present in the data package. Callers
 * are responsible for deciding whether absence is a programming error (required table) or an
 * expected dataset variation (optional table):
 *
 * <pre>{@code
 * // Required table — absence is a routing bug in the orchestrator
 * Dataset<Row> eventDf = loader.load("event")
 *     .orElseThrow(() -> new IllegalStateException(
 *         "event table missing — orchestrator should not have routed here"));
 *
 * // Optional table — absence is a legitimate dataset variation
 * Optional<Dataset<Row>> organismDf = loader.load("organism");
 * }</pre>
 */
@FunctionalInterface
public interface TableLoader {

  Optional<Dataset<Row>> load(String tableName);
}
