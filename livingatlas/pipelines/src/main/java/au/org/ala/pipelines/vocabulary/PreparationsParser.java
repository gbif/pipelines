package au.org.ala.pipelines.vocabulary;

import java.io.FileNotFoundException;
import java.io.InputStream;
import org.gbif.common.parsers.core.FileBasedDictionaryParser;

public class PreparationsParser extends FileBasedDictionaryParser<String> {

  private static PreparationsParser singletonObject = null;

  private PreparationsParser(InputStream file) {
    super(false);
    init(file);
  }

  public static PreparationsParser getInstance(String dictFile) throws FileNotFoundException {
    synchronized (PreparationsParser.class) {
      if (singletonObject == null) {
        String filePath = dictFile;
        if (dictFile == null) {
          filePath = "/preparations.tsv";
        }
        InputStream in = PreparationsParser.class.getResourceAsStream(filePath);
        if (in == null) {
          throw new FileNotFoundException(filePath);
        }
        singletonObject = new PreparationsParser(in);
      }
    }
    return singletonObject;
  }

  @Override
  protected String fromDictFile(String s) {
    return s;
  }
}
