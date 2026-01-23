package org.gbif.pipelines.interpretation;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;

public class Metrics {

  public static final Gauge LAST_CONSUMED_MESSAGE_FROM_QUEUE_MS =
      Gauge.builder()
          .name("last_consumed_message_timestamp_milliseconds")
          .help("Timestamp of the last consumed dataset message from the queue in milliseconds")
          .register();

  public static final Gauge LAST_COMPLETED_MESSAGE_MS =
      Gauge.builder()
          .name("last_completed_dataset_timestamp_milliseconds")
          .help("Timestamp of the last completed dataset message in milliseconds")
          .register();

  // Create a gauge metric
  public static final Gauge CONCURRENT_DATASETS =
      Gauge.builder()
          .name("concurrent_datasets")
          .help("Number of datasets being interpreted concurrently in a pod")
          .register();

  // Create a gauge metric
  public static final Counter MESSAGES_READ_FROM_QUEUE =
      Counter.builder()
          .name("messages_read_from_queue")
          .help("Number of completed datasets being interpreted concurrently in a pod")
          .register();

  // Create a gauge metric
  public static final Counter COMPLETED_DATASETS =
      Counter.builder()
          .name("completed_datasets")
          .help("Number of completed datasets being interpreted concurrently in a pod")
          .register();

  public static final Counter DATASETS_ERRORED_COUNT =
      Counter.builder()
          .name("datasets_errored_count")
          .help("Number of errors thrown during processing")
          .register();

  public static final Gauge LAST_DATASETS_ERROR =
      Gauge.builder()
          .name("last_dataset_error_timestamp_milliseconds")
          .help("Timestamp of the last error thrown during dataset processing in milliseconds")
          .register();
}
