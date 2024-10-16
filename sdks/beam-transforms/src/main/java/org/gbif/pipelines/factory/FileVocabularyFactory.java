package org.gbif.pipelines.factory;

import static java.util.Objects.requireNonNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.VocabularyConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService.VocabularyServiceBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
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
public class FileVocabularyFactory implements Serializable {

  private final PipelinesConfig config;
  private final HdfsConfigs hdfsConfigs;

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
  public SerializableSupplier<VocabularyService> getInstanceSupplier() {

    return () -> {
      VocabularyConfig vocabularyConfig = requireNonNull(config.getVocabularyConfig());
      String path = vocabularyConfig.getVocabulariesPath();

      VocabularyServiceBuilder serviceBuilder = VocabularyService.builder();

      vocabularyConfig
          .getVocabulariesNames()
          .forEach(
              (term, name) -> {
                try (InputStream is = readFile(hdfsConfigs, path, name)) {
                  InMemoryVocabularyLookupBuilder builder =
                      InMemoryVocabularyLookup.newBuilder().from(is);
                  if (term.equals(DwcTerm.lifeStage.qualifiedName())) {
                    builder.withPrefilter(PreFilters.REMOVE_NUMERIC_PREFIX);
                  } else if (term.equals(DwcTerm.sex.qualifiedName())) {
                    builder.withPrefilter(PreFilters.REMOVE_NUMERIC);
                  }

                  serviceBuilder.vocabularyLookup(term, builder.build());
                } catch (IOException ex) {
                  throw new PipelinesException(ex);
                }
              });

      return serviceBuilder.build();
    };
  }

  /**
   * Reads a vocabulary file from HDFS/Local FS
   *
   * @param hdfsConfigs HDFS site and core site config file
   * @param vocabulariesDir dir where the vocabulary files are
   * @param vocabularyName name of the vocabulary. It has to be the same as the one used in the file
   *     name.
   * @return {@link InputStream}
   */
  @SneakyThrows
  private static InputStream readFile(
      HdfsConfigs hdfsConfigs, String vocabulariesDir, String vocabularyName) {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, vocabulariesDir);
    Path fPath = new Path(String.join(Path.SEPARATOR, vocabulariesDir, vocabularyName + ".json"));
    if (fs.exists(fPath)) {
      log.info("Reading vocabularies path - {}", fPath);
      return fs.open(fPath);
    }

    throw new FileNotFoundException("The vocabulary file doesn't exist - " + fPath);
  }
}
