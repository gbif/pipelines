package org.gbif.pipelines.validator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.ClosableIterator;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.TermInfo;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaFileTermCounter {

  public static List<FileInfo> process(Archive archive) {

    List<FileInfo> result = new ArrayList<>();
    result.add(process(archive.getCore(), DwcFileType.CORE));
    archive.getExtensions().forEach(ext -> result.add(process(ext, DwcFileType.EXTENSION)));
    return result;
  }

  private static FileInfo process(ArchiveFile archiveFile, DwcFileType fileType) {
    long fileCount = 0;

    HashMap<Term, Long> termSizeMap = new HashMap<>(archiveFile.getTerms().size());

    try (ClosableIterator<Record> iterator = archiveFile.iterator()) {
      while (iterator.hasNext()) {
        fileCount++;
        Record record = iterator.next();
        record
            .terms()
            .forEach(
                term -> {
                  String value = record.value(term);
                  if (value == null || value.trim().isEmpty()) {
                    termSizeMap.putIfAbsent(term, 0L);
                  } else {
                    Long termCount = termSizeMap.get(term);
                    if (termCount == null || termCount == 0L) {
                      termSizeMap.put(term, 1L);
                    } else {
                      termSizeMap.put(term, termCount + 1);
                    }
                  }
                });
      }
    } catch (Exception ex) {
      log.error(ex.getMessage());
    }

    List<TermInfo> termInfoList =
        termSizeMap.entrySet().stream()
            .map(
                es ->
                    TermInfo.builder()
                        .term(es.getKey().qualifiedName())
                        .rawIndexed(es.getValue())
                        .build())
            .collect(Collectors.toList());

    return FileInfo.builder()
        .count(fileCount)
        .fileName(archiveFile.getTitle())
        .fileType(fileType)
        .rowType(archiveFile.getRowType().qualifiedName())
        .terms(termInfoList)
        .build();
  }
}
