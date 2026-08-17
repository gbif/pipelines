package org.gbif.pipelines.core.io;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

@Slf4j
@Builder
@SuppressWarnings("all")
public class ElasticsearchWriter<T> {

  private String[] esHosts;
  private int syncModeThreshold;
  private Function<T, Document> indexRequestFn;
  private ExecutorService executor;
  private Collection<T> records;
  private long esMaxBatchSize;
  private long esMaxBatchSizeBytes;
  private Integer backPressure;

  @Value
  @Builder
  public static class Document {
    String index;
    String id;
    String source;
  }

  private static class Batch {
    private final List<BulkOperation> operations = new ArrayList<>();
    private long bytes;

    private void add(Document doc) {
      operations.add(
          BulkOperation.of(
              op ->
                  op.index(
                      idx ->
                          idx.index(doc.getIndex())
                              .id(doc.getId())
                              .document(JsonData.fromJson(doc.getSource())))));
      bytes += doc.getSource().length();
    }

    private boolean isEmpty() {
      return operations.isEmpty();
    }

    private boolean isFull(long maxSize, long maxBytes) {
      return operations.size() > maxSize - 1 || bytes > maxBytes;
    }
  }

  @SneakyThrows
  public void write() {

    boolean useSyncMode = syncModeThreshold > records.size();

    HttpHost[] hosts = Arrays.stream(esHosts).map(HttpHost::create).toArray(HttpHost[]::new);
    try (RestClient restClient = RestClient.builder(hosts).build();
        ElasticsearchClient client =
            new ElasticsearchClient(
                new RestClientTransport(restClient, new JacksonJsonpMapper()))) {

      final Phaser phaser = new Phaser(1);

      final Queue<Batch> requests = new LinkedBlockingQueue<>();
      requests.add(new Batch());

      Consumer<T> addIndexRequestFn =
          br ->
              Optional.ofNullable(requests.peek())
                  .ifPresent(req -> req.add(indexRequestFn.apply(br)));

      Consumer<Batch> clientBulkFn =
          br -> {
            try {
              log.info("Push ES request, number of actions - {}", br.operations.size());
              BulkResponse bulk =
                  client.bulk(
                      BulkRequest.of(b -> b.operations(br.operations).timeout(t -> t.time("5m"))));
              phaser.arrive();
              if (Boolean.TRUE.equals(bulk.errors())) {
                String failure =
                    bulk.items().stream()
                        .filter(item -> item.error() != null)
                        .map(item -> item.error().reason())
                        .collect(Collectors.joining("; "));
                log.error(failure);
                throw new IllegalStateException(failure);
              }
            } catch (IOException ex) {
              log.error(ex.getMessage(), ex);
              throw new IllegalStateException(ex.getMessage(), ex);
            }
          };

      Runnable pushIntoEsFn =
          () ->
              Optional.ofNullable(requests.poll())
                  .filter(req -> !req.isEmpty())
                  .ifPresent(
                      req -> {
                        phaser.register();
                        if (useSyncMode) {
                          clientBulkFn.accept(req);
                        } else {
                          CompletableFuture.runAsync(() -> clientBulkFn.accept(req), executor);
                        }
                      });

      for (T t : records) {
        addIndexRequestFn.accept(t);
        Batch peek = requests.peek();
        if (peek == null || peek.isFull(esMaxBatchSize, esMaxBatchSizeBytes)) {
          checkBackpressure(useSyncMode, phaser);
          pushIntoEsFn.run();
          requests.add(new Batch());
        }
      }

      pushIntoEsFn.run();

      log.info("Waiting for all threads to arrive...");
      phaser.arriveAndAwaitAdvance();
      log.info("Writing data to ES has been finished");
    }
  }

  /**
   * If the mode is async, check back pressure, the number of running async tasks must be less than
   * backPressure setting
   */
  private void checkBackpressure(boolean useSyncMode, Phaser phaser) {
    if (!useSyncMode && backPressure != null && backPressure > 0) {
      while (phaser.getUnarrivedParties() > backPressure) {
        log.info("Back pressure barrier: too many rows waiting...");
        try {
          TimeUnit.MILLISECONDS.sleep(10000L);
        } catch (InterruptedException ex) {
          log.warn("Back pressure barrier", ex);
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
