package au.org.ala.pipelines.vocabulary;

import com.google.common.base.Strings;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/** The vocabulary for sensitivity terms. */
public class Sensitivity {
  private static Vocab sensitivityVocab;

  protected static void clear() {
    sensitivityVocab = null;
  }

  public static Vocab getInstance(String stateVocabFile) throws FileNotFoundException {
    InputStream is;
    if (sensitivityVocab == null) {
      if (Strings.isNullOrEmpty(stateVocabFile)) {
        String sourceClasspathFile = "/sensitivities.txt";
        is = Vocab.class.getResourceAsStream(sourceClasspathFile);
      } else {
        File externalFile = new File(stateVocabFile);
        is = new FileInputStream(externalFile);
      }
      sensitivityVocab = Vocab.loadVocabFromStream(is);
    }
    return sensitivityVocab;
  }
}
