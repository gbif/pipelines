package org.gbif.pipelines.interpretation.transform;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.core.CoreInterpreter;
import org.gbif.pipelines.core.interpreters.core.VocabularyInterpreter;
import org.gbif.pipelines.interpretation.transform.utils.VocabularyServiceFactory;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

public class EventCoreTransform implements Serializable {

  private final PipelinesConfig config;

  private EventCoreTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static EventCoreTransform create(PipelinesConfig config) {
    return new EventCoreTransform(config);
  }

  public EventCoreRecord convert(
      ExtendedRecord source, Map<String, Map<String, String>> erWithParents) {
    if (source == null || source.getCoreTerms().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord is null or empty");
    }

    var vocabServiceUrl = config.getVocabularyService().getWsUrl();
    var vocabularyService = VocabularyServiceFactory.getInstance(vocabServiceUrl);

    EventCoreRecord r =
        EventCoreRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    VocabularyInterpreter.interpretEventType(vocabularyService).accept(source, r);

    CoreInterpreter.interpretReferences(source, r, r::setReferences);
    CoreInterpreter.interpretSampleSizeUnit(source, r::setSampleSizeUnit);
    CoreInterpreter.interpretSampleSizeValue(source, r::setSampleSizeValue);
    CoreInterpreter.interpretLicense(source, r::setLicense);
    CoreInterpreter.interpretDatasetID(source, r::setDatasetID);
    CoreInterpreter.interpretDatasetName(source, r::setDatasetName);
    CoreInterpreter.interpretSamplingProtocol(source, r::setSamplingProtocol);
    CoreInterpreter.interpretParentEventID(source, r::setParentEventID);
    CoreInterpreter.interpretLocationID(source, r::setLocationID);

    return r;
  }
}
