package org.gbif.pipeleins.identifier;

import java.util.UUID;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class PublisherProvidedUniqueIdentifier implements UniqueIdentifier {

  private final UUID datasetKey;
  private final String publisherProvidedIdentifier;


  public PublisherProvidedUniqueIdentifier(UUID datasetKey, String publisherProvidedIdentifier) {
    this.datasetKey = checkNotNull(datasetKey, "datasetKey can't be null");
    this.publisherProvidedIdentifier =
      checkNotNull(publisherProvidedIdentifier, "publisherProvidedIdentifier can't be null");
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
    return Objects.hashCode(datasetKey, publisherProvidedIdentifier);
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
    return Objects.equal(this.datasetKey, other.datasetKey) && Objects
      .equal(this.publisherProvidedIdentifier, other.publisherProvidedIdentifier);
  }
}
