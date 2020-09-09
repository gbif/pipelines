package org.gbif.pipelines.ingest.java.transforms;

import java.util.Map;
import lombok.Builder;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.SerializableSupplier;

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

  public void setup() {
    transform.setup();
  }

  public void replaceDefaultValues(Map<String, ExtendedRecord> source) {
    if (!transform.getTags().isEmpty()) {
      source.forEach((key, value) -> transform.convert(value).ifPresent(v -> source.put(key, v)));
    }
  }
}
