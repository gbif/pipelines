package org.gbif.converters.parser.xml.identifier;

import java.util.Objects;
import java.util.UUID;

public class PublisherProvidedUniqueIdentifier implements UniqueIdentifier {

  private final UUID datasetKey;
  private final String publisherProvidedIdentifier;

  public PublisherProvidedUniqueIdentifier(UUID datasetKey, String publisherProvidedIdentifier) {
    this.datasetKey = Objects.requireNonNull(datasetKey, "datasetKey can't be null");
    this.publisherProvidedIdentifier =
        Objects.requireNonNull(
            publisherProvidedIdentifier, "publisherProvidedIdentifier can't be null");
  }

  public String getPublisherProvidedIdentifier() {
    return publisherProvidedIdentifier;
  }

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

  @Override
  public int hashCode() {
    return Objects.hash(datasetKey, publisherProvidedIdentifier);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final PublisherProvidedUniqueIdentifier other = (PublisherProvidedUniqueIdentifier) obj;

    return Objects.equals(this.datasetKey, other.datasetKey)
        && Objects.equals(this.publisherProvidedIdentifier, other.publisherProvidedIdentifier);
  }
}
