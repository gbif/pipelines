package org.gbif.pipelines.ingest.java.transforms;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.gbif.api.model.registry.MachineTag;
import org.gbif.pipelines.io.avro.ExtendedRecord;

public class DefaultValuesTransform {

  private final org.gbif.pipelines.transforms.common.DefaultValuesTransform transform;

  private DefaultValuesTransform(String propertiesPath, String datasetId, boolean skipRegistryCalls) {
    this.transform = org.gbif.pipelines.transforms.common.DefaultValuesTransform.create(propertiesPath, datasetId, skipRegistryCalls);
  }

  private DefaultValuesTransform(Properties properties, String datasetId, boolean skipRegistryCalls) {
    this.transform = org.gbif.pipelines.transforms.common.DefaultValuesTransform.create(properties, datasetId, skipRegistryCalls);
  }

  public static DefaultValuesTransform create(String propertiesPath, String datasetId, boolean skipRegistryCalls) {
    return new DefaultValuesTransform(propertiesPath, datasetId, skipRegistryCalls);
  }

  public static DefaultValuesTransform create(Properties properties, String datasetId, boolean skipRegistryCalls) {
    return new DefaultValuesTransform(properties, datasetId, skipRegistryCalls);
  }

  public void replaceDefaultValues(Map<String, ExtendedRecord> source) {
    List<MachineTag> tags = transform.getMachineTags();
    if (!tags.isEmpty()) {
      source.forEach((key, value) -> source.put(key, transform.replaceDefaultValues(value, tags)));
    }
  }

}
