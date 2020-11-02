package au.org.ala.pipelines.vocabulary;

import au.org.ala.pipelines.util.Stemmer;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** A trait for a vocabulary. A vocabulary consists of a set of Terms, each with string variants. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class Vocab {

  private final Set<String> canonicals = new HashSet<>();
  // variant -> canonical
  private final HashMap<String, String> variants = new HashMap<>();
  // stemmed variant -> canonical
  private final HashMap<String, String> stemmedVariants = new HashMap<>();

  public static Vocab loadVocabFromStream(InputStream is) {

    Vocab vocab = new Vocab();
    Stemmer stemmer = new Stemmer();

    new BufferedReader(new InputStreamReader(is))
        .lines()
        .map(String::trim)
        .forEach(
            l -> {
              String[] ss = l.split("\t");

              String canonical = ss[0];
              vocab.canonicals.add(canonical);

              for (String s : ss) {
                vocab.variants.put(s.toLowerCase(), canonical);
                vocab.stemmedVariants.put(stemmer.stem(s.toLowerCase()), canonical);
              }
            });

    if (log.isDebugEnabled()) {
      log.debug(vocab.canonicals.size() + " vocabs/records have been loaded.");
    }
    return vocab;
  }

  /** Match a vocab term. */
  public Optional<String> matchTerm(String searchTerm) {
    Stemmer stemmer = new Stemmer();

    String searchTermLowerCase = searchTerm.toLowerCase();
    String stemmedSearchTerm = stemmer.stem(searchTerm.toLowerCase());

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
