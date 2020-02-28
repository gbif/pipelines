package org.gbif.pipelines.fragmenter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.gbif.pipelines.fragmenter.common.FragmentsUploader;
import org.gbif.pipelines.fragmenter.common.HbaseConfiguration;

import org.apache.hadoop.hbase.client.Row;

import lombok.Builder;

@Builder
public class XmlFragmentsUploader implements FragmentsUploader {

  private HbaseConfiguration config;
  private Path pathToArchive;

  @Override
  public long upload() {

    List<CompletableFuture<Void>> futures = new ArrayList<>();

    Queue<List<Row>> rows = new LinkedBlockingQueue<>();
    rows.add(new ArrayList<>());

    //...

    throw new UnsupportedOperationException("EMPTY!");

  }

}
