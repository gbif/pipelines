package org.gbif.pipelines.transform;

import java.time.Instant;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.core.GrscicollInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transform.utils.GrscicollLookupKvStoreFactory;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;

public class GrscicollTransform implements java.io.Serializable {

  private final PipelinesConfig config;

  private GrscicollTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static GrscicollTransform create(PipelinesConfig config) {
    return new GrscicollTransform(config);
  }

  public GrscicollRecord convert(ExtendedRecord source, MetadataRecord mdr) {

    GrscicollRecord gr =
        GrscicollRecord.newBuilder().setCreated(Instant.now().toEpochMilli()).build();
    KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> kvStore =
        GrscicollLookupKvStoreFactory.getKvStore(config);
    GrscicollInterpreter.grscicollInterpreter(kvStore, mdr).accept(source, gr);
    return gr;
  }
}
