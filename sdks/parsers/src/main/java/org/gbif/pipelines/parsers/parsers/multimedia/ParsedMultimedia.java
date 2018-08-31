package org.gbif.pipelines.parsers.parsers.multimedia;

import org.gbif.pipelines.io.avro.MediaType;

import java.net.URI;
import java.time.temporal.Temporal;

/** Models a parsed multimedia record. */
public class ParsedMultimedia {

  private final MediaType type;
  private final String format;
  private final URI identifier;
  private final URI references;
  private final String title;
  private final String description;
  private final String source;
  private final String audience;
  private final Temporal created;
  private final String creator;
  private final String contributor;
  private final String publisher;
  private final String license;
  private final String rightsHolder;

  private ParsedMultimedia(Builder builder) {
    this.audience = builder.audience;
    this.creator = builder.creator;
    this.type = builder.type;
    this.contributor = builder.contributor;
    this.license = builder.license;
    this.description = builder.description;
    this.created = builder.created;
    this.rightsHolder = builder.rightsHolder;
    this.source = builder.source;
    this.format = builder.format;
    this.identifier = builder.identifier;
    this.publisher = builder.publisher;
    this.title = builder.title;
    this.references = builder.references;
  }

  public MediaType getType() {
    return type;
  }

  public String getFormat() {
    return format;
  }

  public URI getIdentifier() {
    return identifier;
  }

  public URI getReferences() {
    return references;
  }

  public String getTitle() {
    return title;
  }

  public String getDescription() {
    return description;
  }

  public String getSource() {
    return source;
  }

  public String getAudience() {
    return audience;
  }

  public Temporal getCreated() {
    return created;
  }

  public String getCreator() {
    return creator;
  }

  public String getContributor() {
    return contributor;
  }

  public String getPublisher() {
    return publisher;
  }

  public String getLicense() {
    return license;
  }

  public String getRightsHolder() {
    return rightsHolder;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {

    private MediaType type;
    private String format;
    private URI identifier;
    private URI references;
    private String title;
    private String description;
    private String source;
    private String audience;
    private Temporal created;
    private String creator;
    private String contributor;
    private String publisher;
    private String license;
    private String rightsHolder;

    private Builder() {}

    public Builder type(MediaType type) {
      this.type = type;
      return this;
    }

    public Builder format(String format) {
      this.format = format;
      return this;
    }

    public Builder identifier(URI identifier) {
      this.identifier = identifier;
      return this;
    }

    public Builder references(URI references) {
      this.references = references;
      return this;
    }

    public Builder title(String title) {
      this.title = title;
      return this;
    }

    public Builder description(String description) {
      this.description = description;
      return this;
    }

    public Builder source(String source) {
      this.source = source;
      return this;
    }

    public Builder audience(String audience) {
      this.audience = audience;
      return this;
    }

    public Builder created(Temporal created) {
      this.created = created;
      return this;
    }

    public Builder creator(String creator) {
      this.creator = creator;
      return this;
    }

    public Builder contributor(String contributor) {
      this.contributor = contributor;
      return this;
    }

    public Builder publisher(String publisher) {
      this.publisher = publisher;
      return this;
    }

    public Builder license(String license) {
      this.license = license;
      return this;
    }

    public Builder rightsHolder(String rightsHolder) {
      this.rightsHolder = rightsHolder;
      return this;
    }

    public ParsedMultimedia build() {
      return new ParsedMultimedia(this);
    }
  }
}
