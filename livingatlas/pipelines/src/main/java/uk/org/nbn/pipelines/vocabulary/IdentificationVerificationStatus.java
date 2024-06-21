package uk.org.nbn.pipelines.vocabulary;

import au.org.ala.pipelines.vocabulary.Vocab;
import com.google.common.base.Strings;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/** The vocabulary for sensitivity terms. */
public class IdentificationVerificationStatus {
  private static Vocab identificationVerificationStatusVocab;

  protected static void clear() {
    identificationVerificationStatusVocab = null;
  }

  public static Vocab getInstance(String stateVocabFile) throws FileNotFoundException {
    InputStream is;
    if (identificationVerificationStatusVocab == null) {
      if (Strings.isNullOrEmpty(stateVocabFile)) {
        String sourceClasspathFile = "/identificationVerificationStatus.txt";
        is = Vocab.class.getResourceAsStream(sourceClasspathFile);
      } else {
        File externalFile = new File(stateVocabFile);
        is = new FileInputStream(externalFile);
      }
      identificationVerificationStatusVocab = Vocab.loadVocabFromStream(is);
    }
    return identificationVerificationStatusVocab;
  }
}
