package org.gbif.pipelines.transform;

import com.google.common.base.Strings;
import java.io.Serializable;
import java.net.URISyntaxException;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;

@Slf4j
@Builder
public class DefaultValuesTransform implements Serializable {

  private PipelinesConfig config;
  private MetadataRecord metadata;
  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private DefaultValuesTransform(PipelinesConfig config, MetadataRecord metadata) {
    this.config = config;
    this.metadata = metadata;
  }

  public static DefaultValuesTransform create(PipelinesConfig config, MetadataRecord metadata) {
    return new DefaultValuesTransform(config, metadata);
  }

  public ExtendedRecord convert(ExtendedRecord source) {
    if (metadata.getMachineTags() == null || metadata.getMachineTags().isEmpty()) {
      return source;
    }

    metadata
        .getMachineTags()
        .forEach(
            tag -> {
                try {
                    Term term = TERM_FACTORY.findPropertyTerm(tag.getName());
                    String defaultValue = tag.getValue();
                    if (term != null && !Strings.isNullOrEmpty(defaultValue)) {
                        source.getCoreTerms().putIfAbsent(term.qualifiedName(), tag.getValue());
                    }
                } catch (Exception ex) {
                    log.warn("Failed to apply default value for tag: {} with value: {}. Error: {}",
                            tag.getName(), tag.getValue(), ex.getMessage());
                }
            });

    return source;
  }
}
