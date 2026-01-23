package org.gbif.pipelines.io.avro;

public interface Record extends Comparable<Record> {

  String getId();

  default int compareTo(Record o) {
    return this.getId().compareTo(o.getId());
  }
}
