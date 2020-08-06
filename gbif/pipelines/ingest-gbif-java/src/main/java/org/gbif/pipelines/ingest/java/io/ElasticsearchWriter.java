package org.gbif.pipelines.ingest.java.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

@Slf4j
@Builder
public class ElasticsearchWriter<T> {

  private String[] esHosts;
  private boolean useSyncMode;
  private Function<T, IndexRequest> indexRequestFn;
  private ExecutorService executor;
  private Collection<T> records;
  private long esMaxBatchSize;
  private long esMaxBatchSizeBytes;
  private Integer backPressure;

  @SneakyThrows
  public void write() {

    // Create ES client and extra function
    HttpHost[] hosts = Arrays.stream(esHosts).map(HttpHost::create).toArray(HttpHost[]::new);
    try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(hosts))) {

      final Phaser phaser = new Phaser(1);

      final Queue<BulkRequest> requests = new LinkedBlockingQueue<>();
      requests.add(new BulkRequest().timeout(TimeValue.timeValueMinutes(5L)));

      Consumer<T> addIndexRequestFn =
          br ->
              Optional.ofNullable(requests.peek())
                  .ifPresent(req -> req.add(indexRequestFn.apply(br)));

      Consumer<BulkRequest> clientBulkFn =
          br -> {
            try {
              log.info("Push ES request, number of actions - {}", br.numberOfActions());
              BulkResponse bulk = client.bulk(br, RequestOptions.DEFAULT);
              phaser.arrive();
              if (bulk.hasFailures()) {
                log.error(bulk.buildFailureMessage());
                throw new ElasticsearchException(bulk.buildFailureMessage());
              }
            } catch (IOException ex) {
              log.error(ex.getMessage(), ex);
              throw new ElasticsearchException(ex.getMessage(), ex);
            }
          };

      Runnable pushIntoEsFn =
          () ->
              Optional.ofNullable(requests.poll())
                  .filter(req -> req.numberOfActions() > 0)
                  .ifPresent(
                      req -> {
                        phaser.register();
                        if (useSyncMode) {
                          clientBulkFn.accept(req);
                        } else {
                          CompletableFuture.runAsync(() -> clientBulkFn.accept(req), executor);
                        }
                      });

      // Push requests into ES
      for (T t : records) {
        addIndexRequestFn.accept(t);
        BulkRequest peek = requests.peek();
        if (peek == null
            || peek.numberOfActions() > esMaxBatchSize - 1
            || peek.estimatedSizeInBytes() > esMaxBatchSizeBytes) {
          checkBackpressure(phaser);
          pushIntoEsFn.run();
          requests.add(new BulkRequest().timeout(TimeValue.timeValueMinutes(5L)));
        }
      }

      // Final push
      pushIntoEsFn.run();

      // Wait for all futures
      phaser.arriveAndAwaitAdvance();
    }
  }

  /**
   * If the mode is async, check back pressure, the number of running async tasks must be less than
   * backPressure setting
   */
  private void checkBackpressure(Phaser phaser) {
    if (!useSyncMode && backPressure != null && backPressure > 0) {
      while (phaser.getUnarrivedParties() > backPressure) {
        log.info("Back pressure barrier: too many rows wainting...");
        try {
          TimeUnit.MILLISECONDS.sleep(200L);
        } catch (InterruptedException ex) {
          log.warn("Back pressure barrier", ex);
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
