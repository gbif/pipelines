package org.gbif.pipelines.core.factory;

import static java.util.Objects.requireNonNull;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.Terms;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.VocabularyConfig;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.vocabulary.lookup.InMemoryVocabularyLookup;
import org.gbif.vocabulary.lookup.InMemoryVocabularyLookup.InMemoryVocabularyLookupBuilder;
import org.gbif.vocabulary.lookup.PreFilters;
import org.gbif.vocabulary.lookup.VocabularyLookup;

/**
 * Factory to create instances of {@link VocabularyLookup} from a file containing an exported
 * vocabulary.
 */
@Slf4j
@Builder
public class FileVocabularyFactory {

  private final PipelinesConfig config;
  private final String hdfsSiteConfig;
  private final String coreSiteConfig;

  @Builder.Default private Map<Term, VocabularyLookup> vocabularyLookupMap = new HashMap<>();

  /**
   * Creates instances of {@link VocabularyLookup} from a file containing an exported vocabulary.
   *
   * <p>The lookups for some terms are customized to apply certain filters before performing the
   * lookup:
   *
   * <ul>
   *   <li>LifeStage uses a {@link PreFilters#REMOVE_NUMERIC_PREFIX} filter. This filter removes all
   *       the number characters that are present at the beginning of the value.
   * </ul>
   */
  @SneakyThrows
  public void init() {
    VocabularyConfig vocabularyConfig = requireNonNull(config.getVocabularyConfig());
    String path = vocabularyConfig.getVocabulariesPath();

    for (Term term : Terms.getVocabularyBackedTerms()) {
      String fileName = vocabularyConfig.getVocabularyFileName(term);
      try (InputStream is = readFile(hdfsSiteConfig, coreSiteConfig, path, fileName)) {
        InMemoryVocabularyLookupBuilder builder = InMemoryVocabularyLookup.newBuilder().from(is);
        if (term == DwcTerm.lifeStage) {
          builder.withPrefilter(PreFilters.REMOVE_NUMERIC_PREFIX);
        }

        vocabularyLookupMap.put(term, builder.build());
      }
    }
  }

  public void close() {
    vocabularyLookupMap.values().forEach(VocabularyLookup::close);
  }

  public VocabularyLookup getVocabularyLookup(Term term) {
    if (Terms.getVocabularyBackedTerms().contains(term)) {
      return vocabularyLookupMap.get(term);
    }
    throw new IllegalArgumentException("Vocabulary-backed term not supported: " + term);
  }

  /**
   * Reads a vocabulary file from HDFS/Local FS
   *
   * @param hdfsSiteConfig HDFS site config file
   * @param coreSiteConfig HDFS core site config file
   * @param vocabulariesDir dir where the vocabulary files are
   * @param vocabularyName name of the vocabulary. It has to be the same as the one used in the file
   *     name.
   * @return {@link InputStream}
   */
  @SneakyThrows
  private static InputStream readFile(
      String hdfsSiteConfig, String coreSiteConfig, String vocabulariesDir, String vocabularyName) {
    FileSystem fs = FsUtils.getFileSystem(hdfsSiteConfig, coreSiteConfig, vocabulariesDir);
    Path fPath = new Path(String.join(Path.SEPARATOR, vocabulariesDir, vocabularyName + ".json"));
    if (fs.exists(fPath)) {
      log.info("Reading vocabularies path - {}", fPath);
      return fs.open(fPath);
    }

    throw new FileNotFoundException("The vocabulary file doesn't exist - " + fPath);
  }
}
