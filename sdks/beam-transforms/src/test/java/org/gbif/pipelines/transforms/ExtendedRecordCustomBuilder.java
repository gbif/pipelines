package org.gbif.pipelines.transforms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Builder for a {@link ExtendedRecord}.
 *
 * <p>Recommended for testing purposes.
 */
@NoArgsConstructor(staticName = "create")
public class ExtendedRecordCustomBuilder {

  private String name;
  private String id;
  private Map<String, List<Map<String, String>>> extensions;

  public ExtendedRecordCustomBuilder name(String name) {
    this.name = name;
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

    addToMap(terms, DwcTerm.scientificName, name);

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

  @Builder
  public static class MultimediaExtensionBuilder {

    // AUDUBON Extension
    private String format;
    private String identifier;
    private String title;
    private String description;
    private String type;
    private String license;
    private String source;
    private String creator;
    private String created;

    public Map<String, String> toMap() {
      Map<String, String> Extension = new HashMap<>();
      addToMap(Extension, DcTerm.format, format);
      addToMap(Extension, DcTerm.identifier, identifier);
      addToMap(Extension, DcTerm.title, title);
      addToMap(Extension, DcTerm.description, description);
      addToMap(Extension, DcTerm.license, license);
      addToMap(Extension, DcTerm.source, source);
      addToMap(Extension, DcTerm.creator, creator);
      addToMap(Extension, DcTerm.created, created);
      addToMap(Extension, DcTerm.type, type);

      return Extension;
    }
  }
}
