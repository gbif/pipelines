package org.gbif.pipelines.transforms;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for a {@link ExtendedRecord}.
 *
 * <p>Recommended for testing purposes.
 */
public class ExtendedRecordCustomBuilder {

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

  private ExtendedRecordCustomBuilder() {}

  public static ExtendedRecordCustomBuilder create() {
    return new ExtendedRecordCustomBuilder();
  }

  public static MultimediaExtensionBuilder createMultimediaExtensionBuilder() {
    return new MultimediaExtensionBuilder();
  }

  public ExtendedRecordCustomBuilder country(String country) {
    this.country = country;
    return this;
  }

  public ExtendedRecordCustomBuilder countryCode(String countryCode) {
    this.countryCode = countryCode;
    return this;
  }

  public ExtendedRecordCustomBuilder decimalLatitude(String decimalLatitude) {
    this.decimalLatitude = decimalLatitude;
    return this;
  }

  public ExtendedRecordCustomBuilder decimalLongitude(String decimalLongitude) {
    this.decimalLongitude = decimalLongitude;
    return this;
  }

  public ExtendedRecordCustomBuilder verbatimLatitude(String verbatimLatitude) {
    this.verbatimLatitude = verbatimLatitude;
    return this;
  }

  public ExtendedRecordCustomBuilder verbatimLongitude(String verbatimLongitude) {
    this.verbatimLongitude = verbatimLongitude;
    return this;
  }

  public ExtendedRecordCustomBuilder verbatimCoords(String verbatimCoords) {
    this.verbatimCoords = verbatimCoords;
    return this;
  }

  public ExtendedRecordCustomBuilder geodeticDatum(String geodeticDatum) {
    this.geodeticDatum = geodeticDatum;
    return this;
  }

  public ExtendedRecordCustomBuilder kingdom(String kingdom) {
    this.kingdom = kingdom;
    return this;
  }

  public ExtendedRecordCustomBuilder phylum(String phylum) {
    this.phylum = phylum;
    return this;
  }

  public ExtendedRecordCustomBuilder clazz(String clazz) {
    this.clazz = clazz;
    return this;
  }

  public ExtendedRecordCustomBuilder order(String order) {
    this.order = order;
    return this;
  }

  public ExtendedRecordCustomBuilder family(String family) {
    this.family = family;
    return this;
  }

  public ExtendedRecordCustomBuilder genus(String genus) {
    this.genus = genus;
    return this;
  }

  public ExtendedRecordCustomBuilder rank(String rank) {
    this.rank = rank;
    return this;
  }

  public ExtendedRecordCustomBuilder name(String name) {
    this.name = name;
    return this;
  }

  public ExtendedRecordCustomBuilder authorship(String authorship) {
    this.authorship = authorship;
    return this;
  }

  public ExtendedRecordCustomBuilder specificEpithet(String specificEpithet) {
    this.specificEpithet = specificEpithet;
    return this;
  }

  public ExtendedRecordCustomBuilder infraspecificEpithet(String infraspecificEpithet) {
    this.infraspecificEpithet = infraspecificEpithet;
    return this;
  }

  public ExtendedRecordCustomBuilder associatedMedia(String associatedMedia) {
    this.associatedMedia = associatedMedia;
    return this;
  }

  public ExtendedRecordCustomBuilder id(String id) {
    this.id = id;
    return this;
  }

  public ExtendedRecordCustomBuilder addExtensionRecord(
      Extension extension, Map<String, String> record) {
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
      Map<String, String> Extension = new HashMap<>();
      addToMap(Extension, DcTerm.format, format);
      addToMap(Extension, AcTerm.accessURI, accessURI);
      addToMap(Extension, DcTerm.identifier, identifier);
      addToMap(Extension, DcTerm.references, references);
      addToMap(Extension, AcTerm.furtherInformationURL, furtherInformationURL);
      addToMap(Extension, AcTerm.attributionLinkURL, attributionLinkURL);
      addToMap(Extension, DcTerm.title, title);
      addToMap(Extension, DcTerm.description, description);
      addToMap(Extension, AcTerm.caption, caption);
      addToMap(Extension, DcTerm.license, license);
      addToMap(Extension, DcTerm.rights, rights);
      addToMap(Extension, DcTerm.publisher, publisher);
      addToMap(Extension, DcTerm.contributor, contributor);
      addToMap(Extension, DcTerm.source, source);
      addToMap(Extension, AcTerm.derivedFrom, derivedFrom);
      addToMap(Extension, DcTerm.audience, audience);
      addToMap(Extension, DcTerm.rightsHolder, rightsHolder);
      addToMap(Extension, DcTerm.creator, creator);
      addToMap(Extension, DcTerm.created, created);

      return Extension;
    }
  }
}
