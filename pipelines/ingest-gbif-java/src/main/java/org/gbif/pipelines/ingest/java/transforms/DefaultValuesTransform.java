package org.gbif.pipelines.ingest.java.transforms;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.gbif.api.model.registry.MachineTag;
import org.gbif.pipelines.io.avro.ExtendedRecord;

public class DefaultValuesTransform {

  private final org.gbif.pipelines.transforms.DefaultValuesTransform transform;

  private DefaultValuesTransform(String propertiesPath, String datasetId) {
    this.transform = org.gbif.pipelines.transforms.DefaultValuesTransform.create(propertiesPath, datasetId);
  }

  private DefaultValuesTransform(Properties properties, String datasetId) {
    this.transform = org.gbif.pipelines.transforms.DefaultValuesTransform.create(properties, datasetId);
  }

  public static DefaultValuesTransform create(String propertiesPath, String datasetId) {
    return new DefaultValuesTransform(propertiesPath, datasetId);
  }

  public static DefaultValuesTransform create(Properties properties, String datasetId) {
    return new DefaultValuesTransform(properties, datasetId);
  }

  public HashMap<String, ExtendedRecord> replaceDefaultValues(HashMap<String, ExtendedRecord> source) {
    List<MachineTag> tags = transform.getMachineTags();
    if (!tags.isEmpty()) {
      source.forEach((key, value) -> source.put(key, transform.replaceDefaultValues(value, tags)));
    }
    return source;
  }

}
