package au.org.ala.pipelines.vocabulary;

import com.google.common.base.Strings;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Load a list of state names and variants For example: Northern Territory nterritory nterrit nt
 * Northern Territory (including Coastal Waters)
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StateProvince {
  private static Vocab stateProvinceVocab;

  public static Vocab getInstance(String stateVocabFile) throws FileNotFoundException {
    InputStream is;
    if (stateProvinceVocab == null) {
      if (Strings.isNullOrEmpty(stateVocabFile)) {
        String sourceClasspathFile = "/stateProvinces.txt";
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
