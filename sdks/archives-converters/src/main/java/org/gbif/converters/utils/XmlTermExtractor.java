package org.gbif.converters.utils;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ExtendedRecordConverter;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.common.pojo.FileNameTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@AllArgsConstructor
@Getter
public class XmlTermExtractor {

  private final Map<FileNameTerm, Set<Term>> core;
  private final Map<FileNameTerm, Set<Term>> extenstionsTerms;

  public XmlTermExtractor extract(File file) {
    return extract(Collections.singletonList(file));
  }

  public static XmlTermExtractor extract(List<File> files) {

    List<ExtendedRecord> records =
        files.stream()
            .map(new OccurrenceParser()::parseFile)
            .flatMap(Collection::stream)
            .map(XmlFragmentParser::parseRecord)
            .flatMap(Collection::stream)
            .map(ExtendedRecordConverter::from)
            .collect(Collectors.toList());

    Set<Term> collect =
        records.stream()
            .map(ExtendedRecord::getCoreTerms)
            .map(XmlTermExtractor::extractTerms)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

    Map<FileNameTerm, Set<Term>> extenstionsTerms = new HashMap<>();
    records.stream()
        .map(record -> record.getExtensions().entrySet())
        .flatMap(Collection::stream)
        .forEach(
            es -> {
              Set<Term> extTerms =
                  es.getValue().stream()
                      .map(XmlTermExtractor::extractTerms)
                      .flatMap(Collection::stream)
                      .collect(Collectors.toSet());
              extenstionsTerms.put(
                  FileNameTerm.create(es.getKey() + ".file", es.getKey()), extTerms);
            });

    return new XmlTermExtractor(
        Collections.singletonMap(
            FileNameTerm.create("core.file", DwcTerm.Occurrence.qualifiedName()), collect),
        extenstionsTerms);
  }

  private static Set<Term> extractTerms(Map<String, String> map) {
    return map.entrySet().stream()
        .filter(z -> z.getValue() != null && !z.getValue().isEmpty())
        .map(Entry::getKey)
        .map(TermFactory.instance()::findTerm)
        .collect(Collectors.toSet());
  }
}
