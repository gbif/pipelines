package au.org.ala.pipelines.vocabulary;

import au.org.ala.pipelines.util.Stemmer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/** A trait for a vocabulary. A vocabulary consists of a set of Terms, each with string variants. */
@Slf4j
public class Vocab {

  private Set<String> canonicals = new HashSet<String>();
  // variant -> canonical
  private HashMap<String, String> variants = new HashMap<String, String>();
  // stemmed variant -> canonical
  private HashMap<String, String> stemmedVariants = new HashMap<String, String>();

  private Vocab() {}

  public static Vocab loadVocabFromFile(String vocabfile) throws FileNotFoundException {
    InputStream is;
    File externalFile = new File(vocabfile);
    is = new FileInputStream(externalFile);
    return Vocab.loadVocabFromStream(is);
  }

  public static Vocab loadVocabFromStream(InputStream is) {

    Vocab vocab = new Vocab();
    Stemmer stemmer = new Stemmer();

    new BufferedReader(new InputStreamReader(is))
        .lines()
        .map(s -> s.trim())
        .forEach(
            l -> {
              String[] ss = l.split("\t");

              String canonical = ss[0];
              vocab.canonicals.add(canonical);

              for (int i = 0; i < ss.length; i++) {
                vocab.variants.put(ss[i].toLowerCase(), canonical);
                vocab.stemmedVariants.put(stemmer.stem(ss[i].toLowerCase()), canonical);
              }
            });

    if (log.isDebugEnabled()) {
      log.debug(vocab.canonicals.size() + " vocabs/records have been loaded.");
    }
    return vocab;
  }

  /**
   * Match a vocab term.
   *
   * @param searchTerm
   * @return
   */
  public Optional<String> matchTerm(String searchTerm) {
    Stemmer stemmer = new Stemmer();

    String searchTermLowerCase = searchTerm.toLowerCase();
    String stemmedSearchTerm = stemmer.stem(searchTerm.toLowerCase());

    String[] result = null;

    // match by key
    if (canonicals.contains(searchTerm)) {
      return Optional.of(searchTerm);
    }

    // match by key
    if (variants.containsKey(searchTermLowerCase)) {
      return Optional.of(variants.get(searchTerm.toLowerCase()));
    }

    if (stemmedVariants.containsKey(stemmedSearchTerm)) {
      return Optional.of(stemmedVariants.get(stemmedSearchTerm));
    }

    return Optional.empty();
  }
}
