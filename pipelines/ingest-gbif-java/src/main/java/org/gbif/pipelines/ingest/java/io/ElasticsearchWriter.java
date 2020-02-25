package org.gbif.pipelines.ingest.java.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import org.gbif.pipelines.io.avro.BasicRecord;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class ElasticsearchWriter {

  private String[] esHosts;
  private boolean useSyncMode;
  private Function<BasicRecord, IndexRequest> indexRequestFn;
  private ExecutorService executor;
  private Collection<BasicRecord> records;
  private long esMaxBatchSize;
  private long esMaxBatchSizeBytes;

  @SneakyThrows
  public void write() {

    // Create ES client and extra function
    HttpHost[] hosts = Arrays.stream(esHosts).map(HttpHost::create).toArray(HttpHost[]::new);
    try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(hosts))) {

      List<CompletableFuture<Void>> futures = new ArrayList<>();

      Queue<BulkRequest> requests = new LinkedList<>();
      requests.add(new BulkRequest().timeout(TimeValue.timeValueMinutes(5L)));

      Consumer<BasicRecord> addIndexRequestFn = br -> Optional.ofNullable(requests.peek())
          .ifPresent(req -> req.add(indexRequestFn.apply(br)));

      Consumer<BulkRequest> clientBulkFn = br -> {
        try {
          if (br.numberOfActions() > 0) {
            BulkResponse bulk = client.bulk(br, RequestOptions.DEFAULT);
            if (bulk.hasFailures()) {
              log.error(bulk.buildFailureMessage());
              throw new ElasticsearchException(bulk.buildFailureMessage());
            }
          }
        } catch (IOException ex) {
          log.error(ex.getMessage(), ex);
          throw new ElasticsearchException(ex.getMessage(), ex);
        }
      };

      Runnable pushIntoEsFn = () -> Optional.ofNullable(requests.poll())
          .ifPresent(req -> {
            log.info("Push ES request, number of actions - {}", req.numberOfActions());
            if (useSyncMode) {
              clientBulkFn.accept(req);
            } else {
              futures.add(CompletableFuture.runAsync(() -> clientBulkFn.accept(req), executor));
            }
          });

      // Push requests into ES
      for (BasicRecord br : records) {
        BulkRequest peek = requests.peek();
        if (peek != null && peek.numberOfActions() < esMaxBatchSize && peek.estimatedSizeInBytes() < esMaxBatchSizeBytes) {
          addIndexRequestFn.accept(br);
        } else {
          addIndexRequestFn.accept(br);
          pushIntoEsFn.run();
          requests.add(new BulkRequest().timeout(TimeValue.timeValueMinutes(5L)));
        }
      }

      pushIntoEsFn.run();

      // Wait for all futures
      if (!useSyncMode) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      }
    }

  }

}
