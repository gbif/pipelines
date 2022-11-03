package org.gbif.converters.parser.xml.identifier;

import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@EqualsAndHashCode
@Getter
@AllArgsConstructor
public class PublisherProvidedUniqueIdentifier implements UniqueIdentifier {

  @NonNull private final UUID datasetKey;
  @NonNull private final String publisherProvidedIdentifier;

  @Override
  public UUID getDatasetKey() {
    return datasetKey;
  }

  @Override
  public String getUniqueString() {
    return OccurrenceKeyHelper.buildKey(this);
  }

  @Override
  public String getUnscopedUniqueString() {
    return OccurrenceKeyHelper.buildUnscopedKey(this);
  }
}
