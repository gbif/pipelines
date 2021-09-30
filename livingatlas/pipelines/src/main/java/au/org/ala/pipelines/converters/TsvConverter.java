package au.org.ala.pipelines.converters;

import com.beust.jcommander.Strings;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.Term;

@NoArgsConstructor(staticName = "create")
public class TsvConverter<T> {

  private final List<KeyTermFn<T>> keyTermFnList = new LinkedList<>();

  public final TsvConverter<T> addKeyTermFn(
      String term, Function<T, Optional<String>> fn, String defaultValue) {
    keyTermFnList.add(KeyTermFn.create(term, fn, defaultValue));
    return this;
  }

  public final TsvConverter<T> addKeyTermFn(
      Term term, Function<T, Optional<String>> fn, String defaultValue) {
    return addKeyTermFn(term.qualifiedName(), fn, defaultValue);
  }

  public final TsvConverter<T> addKeyTermFn(String term, Function<T, Optional<String>> fn) {
    keyTermFnList.add(KeyTermFn.create(term, fn, ""));
    return this;
  }

  public final TsvConverter<T> addKeyTermFn(Term term, Function<T, Optional<String>> fn) {
    return addKeyTermFn(term.qualifiedName(), fn);
  }

  public String converter(T source) {
    final Map<String, String> resultMap = new LinkedHashMap<>();
    keyTermFnList.forEach(
        keyTermFn -> {
          String value = keyTermFn.getFn().apply(source).orElse(keyTermFn.getDefaultValue());
          // replace any tabs within string values with single space
          value = "\"" + value.replaceAll("\t", " ").replaceAll("\"", "") + "\"";
          resultMap.put(keyTermFn.getTerm(), value);
        });
    return Strings.join("\t", resultMap.values().toArray(new String[0]));
  }

  public List<String> getTerms() {
    return keyTermFnList.stream()
        .sequential()
        .map(KeyTermFn::getTerm)
        .collect(Collectors.toCollection(LinkedList::new));
  }

  @Getter
  @AllArgsConstructor(staticName = "create")
  public static class KeyTermFn<T> {
    private final String term;
    private final Function<T, Optional<String>> fn;
    private final String defaultValue;
  }
}
