package org.gbif.pipelines.core.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.json.AgentIdentifier;
import org.gbif.pipelines.io.avro.json.VerbatimRecord;
import org.gbif.pipelines.io.avro.json.VocabularyConcept;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class JsonConverter {

  private static final Set<String> EXCLUDE_ALL =
      Collections.singleton(DwcTerm.footprintWKT.qualifiedName());

  private static final Set<String> INCLUDE_EXT_ALL =
      new HashSet<>(
          Arrays.asList(
              Extension.MULTIMEDIA.getRowType(),
              Extension.AUDUBON.getRowType(),
              Extension.IMAGE.getRowType()));

  private static final Map<Character, Character> CHAR_MAP = new HashMap<>(2);

  static {
    CHAR_MAP.put('\u001E', ',');
    CHAR_MAP.put('\u001f', ' ');
  }

  protected static String getEscapedText(String value) {
    String v = value;
    for (Entry<Character, Character> rule : CHAR_MAP.entrySet()) {
      v = v.replace(rule.getKey(), rule.getValue());
    }
    return v;
  }

  protected static List<String> convertAll(ExtendedRecord extendedRecord) {
    Set<String> result = new HashSet<>();

    extendedRecord.getCoreTerms().entrySet().stream()
        .filter(term -> !EXCLUDE_ALL.contains(term.getKey()))
        .filter(term -> term.getValue() != null)
        .map(Entry::getValue)
        .forEach(result::add);

    extendedRecord.getExtensions().entrySet().stream()
        .filter(kv -> INCLUDE_EXT_ALL.contains(kv.getKey()))
        .map(Entry::getValue)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .flatMap(map -> map.values().stream())
        .filter(Objects::nonNull)
        .forEach(result::add);

    return result.stream()
        .flatMap(v -> Stream.of(v.split(ModelUtils.DEFAULT_SEPARATOR)))
        .map(JsonConverter::getEscapedText)
        .collect(Collectors.toList());
  }

  protected static List<String> convertExtenstions(ExtendedRecord extendedRecord) {
    return extendedRecord.getExtensions().entrySet().stream()
        .filter(e -> e.getValue() != null && !e.getValue().isEmpty())
        .map(Entry::getKey)
        .distinct()
        .collect(Collectors.toList());
  }

  protected static VerbatimRecord convertVerbatimRecord(ExtendedRecord extendedRecord) {
    return VerbatimRecord.newBuilder()
        .setCoreTerms(extendedRecord.getCoreTerms())
        .setExtensions(extendedRecord.getExtensions())
        .build();
  }

  protected static Optional<String> convertToMultivalue(List<String> list) {
    return list != null && !list.isEmpty() ? Optional.of(String.join("|", list)) : Optional.empty();
  }

  protected static Optional<String> convertLicense(String license) {
    return Optional.ofNullable(license)
        .filter(l -> !l.equals(License.UNSPECIFIED.name()))
        .filter(l -> !l.equals(License.UNSUPPORTED.name()));
  }

  protected static List<AgentIdentifier> convertAgentList(
      List<org.gbif.pipelines.io.avro.AgentIdentifier> list) {
    return list.stream()
        .map(x -> AgentIdentifier.newBuilder().setType(x.getType()).setValue(x.getValue()).build())
        .collect(Collectors.toList());
  }

  protected static Optional<VocabularyConcept> convertVocabularyConcept(
      org.gbif.pipelines.io.avro.VocabularyConcept concepts) {
    if (concepts == null) {
      return Optional.empty();
    }
    return Optional.of(
        VocabularyConcept.newBuilder()
            .setConcept(concepts.getConcept())
            .setLineage(concepts.getLineage())
            .build());
  }

  protected static void mapIssues(
      List<Issues> records, Consumer<List<String>> issueFn, Consumer<List<String>> notIssueFn) {
    Set<String> issues =
        records.stream()
            .flatMap(x -> x.getIssues().getIssueList().stream())
            .collect(Collectors.toSet());
    issueFn.accept(new ArrayList<>(issues));

    Set<String> notIssues =
        Arrays.stream(OccurrenceIssue.values())
            .map(Enum::name)
            .filter(x -> !issues.contains(x))
            .collect(Collectors.toSet());
    notIssueFn.accept(new ArrayList<>(notIssues));
  }
}
