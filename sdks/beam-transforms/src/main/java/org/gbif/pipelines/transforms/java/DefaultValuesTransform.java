package org.gbif.pipelines.transforms.java;

import java.util.Map;
import lombok.Builder;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Java transformations to use verbatim default term values defined as MachineTags in an
 * MetadataRecord. transforms form {@link ExtendedRecord} to {@link ExtendedRecord}.
 */
public class DefaultValuesTransform {

  private final org.gbif.pipelines.transforms.metadata.DefaultValuesTransform transform;

  @Builder(buildMethodName = "create")
  private DefaultValuesTransform(
      String datasetId, SerializableSupplier<MetadataServiceClient> clientSupplier) {
    this.transform =
        org.gbif.pipelines.transforms.metadata.DefaultValuesTransform.builder()
            .clientSupplier(clientSupplier)
            .datasetId(datasetId)
            .create();
  }

  public DefaultValuesTransform init() {
    transform.setup();
    return this;
  }

  public void replaceDefaultValues(Map<String, ExtendedRecord> source) {
    if (!transform.getTags().isEmpty()) {
      source.forEach((key, value) -> transform.convert(value).ifPresent(v -> source.put(key, v)));
    }
  }
}
