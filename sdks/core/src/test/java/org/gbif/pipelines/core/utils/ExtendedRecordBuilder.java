package org.gbif.pipelines.core.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Builder for a {@link ExtendedRecord}.
 *
 * <p>Recommended for testing purposes.
 */
public class ExtendedRecordBuilder {

  private String country;
  private String countryCode;
  private String decimalLatitude;
  private String decimalLongitude;
  private String verbatimLatitude;
  private String verbatimLongitude;
  private String verbatimCoords;
  private String geodeticDatum;
  private String kingdom;
  private String phylum;
  private String clazz;
  private String order;
  private String family;
  private String genus;
  private String rank;
  private String name;
  private String authorship;
  private String id;
  private String specificEpithet;
  private String infraspecificEpithet;
  private String associatedMedia;
  private Map<String, List<Map<String, String>>> extensions;

  private ExtendedRecordBuilder() {}

  public static ExtendedRecordBuilder create() {
    return new ExtendedRecordBuilder();
  }

  public static MultimediaExtensionBuilder createMultimediaExtensionBuilder() {
    return new MultimediaExtensionBuilder();
  }

  public ExtendedRecordBuilder country(String country) {
    this.country = country;
    return this;
  }

  public ExtendedRecordBuilder countryCode(String countryCode) {
    this.countryCode = countryCode;
    return this;
  }

  public ExtendedRecordBuilder decimalLatitude(String decimalLatitude) {
    this.decimalLatitude = decimalLatitude;
    return this;
  }

  public ExtendedRecordBuilder decimalLongitude(String decimalLongitude) {
    this.decimalLongitude = decimalLongitude;
    return this;
  }

  public ExtendedRecordBuilder verbatimLatitude(String verbatimLatitude) {
    this.verbatimLatitude = verbatimLatitude;
    return this;
  }

  public ExtendedRecordBuilder verbatimLongitude(String verbatimLongitude) {
    this.verbatimLongitude = verbatimLongitude;
    return this;
  }

  public ExtendedRecordBuilder verbatimCoords(String verbatimCoords) {
    this.verbatimCoords = verbatimCoords;
    return this;
  }

  public ExtendedRecordBuilder geodeticDatum(String geodeticDatum) {
    this.geodeticDatum = geodeticDatum;
    return this;
  }

  public ExtendedRecordBuilder kingdom(String kingdom) {
    this.kingdom = kingdom;
    return this;
  }

  public ExtendedRecordBuilder phylum(String phylum) {
    this.phylum = phylum;
    return this;
  }

  public ExtendedRecordBuilder clazz(String clazz) {
    this.clazz = clazz;
    return this;
  }

  public ExtendedRecordBuilder order(String order) {
    this.order = order;
    return this;
  }

  public ExtendedRecordBuilder family(String family) {
    this.family = family;
    return this;
  }

  public ExtendedRecordBuilder genus(String genus) {
    this.genus = genus;
    return this;
  }

  public ExtendedRecordBuilder rank(String rank) {
    this.rank = rank;
    return this;
  }

  public ExtendedRecordBuilder name(String name) {
    this.name = name;
    return this;
  }

  public ExtendedRecordBuilder authorship(String authorship) {
    this.authorship = authorship;
    return this;
  }

  public ExtendedRecordBuilder specificEpithet(String specificEpithet) {
    this.specificEpithet = specificEpithet;
    return this;
  }

  public ExtendedRecordBuilder infraspecificEpithet(String infraspecificEpithet) {
    this.infraspecificEpithet = infraspecificEpithet;
    return this;
  }

  public ExtendedRecordBuilder associatedMedia(String associatedMedia) {
    this.associatedMedia = associatedMedia;
    return this;
  }

  public ExtendedRecordBuilder id(String id) {
    this.id = id;
    return this;
  }

  public ExtendedRecordBuilder addExtensionRecord(Extension extension, Map<String, String> record) {
    if (extension != null) {
      if (extensions == null) {
        extensions = new HashMap<>();
      }

      extensions.computeIfAbsent(extension.getRowType(), k -> new ArrayList<>());

      extensions.get(extension.getRowType()).add(record);
    }

    return this;
  }

  public ExtendedRecord build() {
    Map<String, String> terms = new HashMap<>();

    addToMap(terms, DwcTerm.country, country);
    addToMap(terms, DwcTerm.countryCode, countryCode);
    addToMap(terms, DwcTerm.decimalLatitude, decimalLatitude);
    addToMap(terms, DwcTerm.decimalLongitude, decimalLongitude);
    addToMap(terms, DwcTerm.verbatimLatitude, verbatimLatitude);
    addToMap(terms, DwcTerm.verbatimLongitude, verbatimLongitude);
    addToMap(terms, DwcTerm.verbatimCoordinates, verbatimCoords);
    addToMap(terms, DwcTerm.geodeticDatum, geodeticDatum);
    addToMap(terms, DwcTerm.kingdom, kingdom);
    addToMap(terms, DwcTerm.genus, genus);
    addToMap(terms, DwcTerm.scientificName, name);
    addToMap(terms, DwcTerm.scientificNameAuthorship, authorship);
    addToMap(terms, DwcTerm.taxonRank, rank);
    addToMap(terms, DwcTerm.phylum, phylum);
    addToMap(terms, DwcTerm.class_, clazz);
    addToMap(terms, DwcTerm.order, order);
    addToMap(terms, DwcTerm.family, family);
    addToMap(terms, DwcTerm.specificEpithet, specificEpithet);
    addToMap(terms, DwcTerm.infraspecificEpithet, infraspecificEpithet);
    addToMap(terms, DwcTerm.associatedMedia, associatedMedia);

    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder().setId(id).setCoreTerms(terms);

    if (extensions != null) {
      builder.setExtensions(extensions);
    }

    return builder.build();
  }

  private static void addToMap(Map<String, String> map, Term term, String value) {
    if (value != null) {
      map.put(term.qualifiedName(), value);
    }
  }

  public static class MultimediaExtensionBuilder {

    // AUDUBON Extension
    private String format;
    private String accessURI;
    private String identifier;
    private String references;
    private String furtherInformationURL;
    private String attributionLinkURL;
    private String title;
    private String description;
    private String caption;
    private String license;
    private String rights;
    private String publisher;
    private String contributor;
    private String source;
    private String derivedFrom;
    private String audience;
    private String rightsHolder;
    private String creator;
    private String created;

    private MultimediaExtensionBuilder() {}

    public MultimediaExtensionBuilder format(String format) {
      this.format = format;
      return this;
    }

    public MultimediaExtensionBuilder accessURI(String accessURI) {
      this.accessURI = accessURI;
      return this;
    }

    public MultimediaExtensionBuilder identifier(String identifier) {
      this.identifier = identifier;
      return this;
    }

    public MultimediaExtensionBuilder references(String references) {
      this.references = references;
      return this;
    }

    public MultimediaExtensionBuilder furtherInformationURL(String furtherInformationURL) {
      this.furtherInformationURL = furtherInformationURL;
      return this;
    }

    public MultimediaExtensionBuilder attributionLinkURL(String attributionLinkURL) {
      this.attributionLinkURL = attributionLinkURL;
      return this;
    }

    public MultimediaExtensionBuilder title(String title) {
      this.title = title;
      return this;
    }

    public MultimediaExtensionBuilder description(String description) {
      this.description = description;
      return this;
    }

    public MultimediaExtensionBuilder caption(String caption) {
      this.caption = caption;
      return this;
    }

    public MultimediaExtensionBuilder license(String license) {
      this.license = license;
      return this;
    }

    public MultimediaExtensionBuilder rights(String rights) {
      this.rights = rights;
      return this;
    }

    public MultimediaExtensionBuilder publisher(String publisher) {
      this.publisher = publisher;
      return this;
    }

    public MultimediaExtensionBuilder contributor(String contributor) {
      this.contributor = contributor;
      return this;
    }

    public MultimediaExtensionBuilder source(String source) {
      this.source = source;
      return this;
    }

    public MultimediaExtensionBuilder derivedFrom(String derivedFrom) {
      this.derivedFrom = derivedFrom;
      return this;
    }

    public MultimediaExtensionBuilder audience(String audience) {
      this.audience = audience;
      return this;
    }

    public MultimediaExtensionBuilder rightsHolder(String rightsHolder) {
      this.rightsHolder = rightsHolder;
      return this;
    }

    public MultimediaExtensionBuilder creator(String creator) {
      this.creator = creator;
      return this;
    }

    public MultimediaExtensionBuilder created(String created) {
      this.created = created;
      return this;
    }

    public Map<String, String> build() {
      Map<String, String> extension = new HashMap<>();
      addToMap(extension, DcTerm.format, format);
      addToMap(extension, AcTerm.accessURI, accessURI);
      addToMap(extension, DcTerm.identifier, identifier);
      addToMap(extension, DcTerm.references, references);
      addToMap(extension, AcTerm.furtherInformationURL, furtherInformationURL);
      addToMap(extension, AcTerm.attributionLinkURL, attributionLinkURL);
      addToMap(extension, DcTerm.title, title);
      addToMap(extension, DcTerm.description, description);
      addToMap(extension, AcTerm.caption, caption);
      addToMap(extension, DcTerm.license, license);
      addToMap(extension, DcTerm.rights, rights);
      addToMap(extension, DcTerm.publisher, publisher);
      addToMap(extension, DcTerm.contributor, contributor);
      addToMap(extension, DcTerm.source, source);
      addToMap(extension, AcTerm.derivedFrom, derivedFrom);
      addToMap(extension, DcTerm.audience, audience);
      addToMap(extension, DcTerm.rightsHolder, rightsHolder);
      addToMap(extension, DcTerm.creator, creator);
      addToMap(extension, DcTerm.created, created);

      return extension;
    }
  }
}
