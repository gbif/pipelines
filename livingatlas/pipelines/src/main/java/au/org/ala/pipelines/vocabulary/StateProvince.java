package au.org.ala.pipelines.vocabulary;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Strings;

/**
 * Load a list of state names and variants For example: Northern Territory nterritory nterrit nt
 * Northern Territory (including Coastal Waters)
 */
@Slf4j
public class StateProvince {
  private static String sourceClasspathFile = "/stateProvinces.txt";
  private static Vocab stateProvinceVocab;

  public static Vocab getInstance(String stateVocabFile) throws FileNotFoundException {
    InputStream is;
    if (stateProvinceVocab == null) {
      if (Strings.isNullOrEmpty(stateVocabFile)) {
        is = Vocab.class.getResourceAsStream(sourceClasspathFile);
      } else {
        File externalFile = new File(stateVocabFile);
        is = new FileInputStream(externalFile);
      }
      stateProvinceVocab = Vocab.loadVocabFromStream(is);
    }
    return stateProvinceVocab;
  }
}
