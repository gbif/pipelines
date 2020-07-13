package au.org.ala.pipelines.vocabulary;

import java.io.FileNotFoundException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StateProvince {

  private static Vocab stateProvinceVocab;

  public static Vocab getInstance(String stateVocabFile) throws FileNotFoundException {
    if (stateProvinceVocab == null) {
        stateProvinceVocab = Vocab.loadVocabFromFile(stateVocabFile, "/stateProvinces.txt");
    }
    return stateProvinceVocab;
  }
}
