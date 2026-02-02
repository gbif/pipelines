package org.gbif.pipelines.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.io.Serializable;
import java.time.Instant;
import org.gbif.api.vocabulary.Extension;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.extension.HumboldtInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.gbif.pipelines.transform.utils.MultiTaxonomyKVSFactory;
import org.gbif.pipelines.transform.utils.VocabularyServiceFactory;
import org.gbif.rest.client.species.NameUsageMatchResponse;

public class HumboldtTransform implements Serializable {

  private final PipelinesConfig config;

  private HumboldtTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static HumboldtTransform create(PipelinesConfig config) {
    return new HumboldtTransform(config);
  }

  public HumboldtRecord convert(ExtendedRecord source) {
    if (source == null || source.getCoreTerms().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord is null or empty");
    }

    var vocabularyService = VocabularyServiceFactory.getInstance(config);

    KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> nameUsageMatchKvStore =
        MultiTaxonomyKVSFactory.getKvStore(config);

    HumboldtInterpreter humboldtInterpreter =
        HumboldtInterpreter.builder()
            .kvStore(nameUsageMatchKvStore)
            .checklistKeys(config.getNameUsageMatchingService().getChecklistKeys())
            .vocabularyService(vocabularyService)
            .create();

    HumboldtRecord r =
        HumboldtRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    if (hasExtension(source, Extension.HUMBOLDT)) {
      humboldtInterpreter.interpret(source, r);
    }
    return r;
  }
}
