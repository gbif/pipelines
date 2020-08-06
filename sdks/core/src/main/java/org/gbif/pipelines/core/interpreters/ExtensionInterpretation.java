package org.gbif.pipelines.core.interpreters;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * The class is designed to simplify extension interpretation process:
 *
 * <pre>
 *   1) Set extension name {@link ExtensionInterpretation#extension(Extension)}
 *   or {@link ExtensionInterpretation#extension(String)}, basically, it will try to filter map by given {@link String} value
 *   2) Add a supplier of a target model {@link ExtensionInterpretation#to(Supplier)}
 *   3) Map the Term to a field in a target model, the order is important, in general, it is a sequence of calls:
 *    {@link TargetHandler#map(Term, BiConsumer)}
 *    {@link TargetHandler#map(String, BiConsumer)}
 *    {@link TargetHandler#map(Term, BiFunction)}
 *    {@link TargetHandler#map(String, BiFunction)}
 *    {@link TargetHandler#mapOne(Term, BiFunction)}
 *    {@link TargetHandler#mapOne(String, BiFunction)}
 *   4) Post mapping, if you want to process two or more fields:
 *    {@link TargetHandler#postMap(Consumer)}
 *    {@link TargetHandler#postMap(Function)}
 *    {@link TargetHandler#postMapOne(Function)}
 *   5) Skip whole record if some important conditions are violated {@link TargetHandler#skipIf(Function)}
 *   6) Use convert and pass a source of data:
 *    {@link TargetHandler#convert(ExtendedRecord)}
 *    {@link TargetHandler#convert(Map)}
 *    {@link TargetHandler#convert(List)}
 *   7) Process the result of the conversion {@link Result}
 * </pre>
 *
 * <p>Example:
 *
 * <pre>{@code
 * private static final TargetHandler<Image> HANDLER =
 *     ExtensionInterpretation.extension(Extension.IMAGE)
 *       .to(Image::new)
 *       .map(DcTerm.identifier, ImageInterpreter::parseAndSetIdentifier)
 *       .mapOne("http://www.w3.org/2003/01/geo/wgs84_pos#longitude", ImageInterpreter::parseAndSetLongitude)
 *       .postMap(ImageInterpreter::parseAndSetLatLng)
 *       .skipIf(ImageInterpreter::checkLinks);
 *
 * Result<Image> result = HANDLER.convert(er);
 * result.getList();
 * result.getIssuesAsList();
 * }</pre>
 *
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.AmplificationInterpreter}
 *
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.AudubonInterpreter}
 *
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.ImageInterpreter}
 *
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.MeasurementOrFactInterpreter}
 *
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter}
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ExtensionInterpretation {

  private final String extension;

  /** @param extension Filter source map by given {@link String} value */
  public static ExtensionInterpretation extension(String extension) {
    return new ExtensionInterpretation(extension);
  }

  /** @param extension Filter source map by given {@link Extension} value */
  public static ExtensionInterpretation extension(Extension extension) {
    return new ExtensionInterpretation(extension.getRowType());
  }

  /** @param supplier of a target object, as example - Image::new */
  public <T> TargetHandler<T> to(Supplier<T> supplier) {
    return new TargetHandler<>(supplier);
  }

  public class TargetHandler<T> {

    private final Supplier<T> supplier;

    private Map<String, BiFunction<T, String, List<String>>> mapperMap = new LinkedHashMap<>();

    private Set<Function<T, List<String>>> postMapperSet = new LinkedHashSet<>();

    private Set<Function<T, Optional<String>>> validatorSet = new LinkedHashSet<>();

    /** @param supplier of a target object, as example - Image::new */
    private TargetHandler(Supplier<T> supplier) {
      this.supplier = supplier;
    }

    /**
     * Maps the {@link Term} to a field in a target model, can't process an issue
     *
     * @param key as a {@link Term} for source map
     * @param consumer, where {@link T} is a target object and {@link String} value by key from
     *     source map
     */
    public TargetHandler<T> map(Term key, BiConsumer<T, String> consumer) {
      return map(key.qualifiedName(), consumer);
    }

    /**
     * Maps the {@link String} to a field in a target model, can't process an issue
     *
     * @param key as a {@link String} for source map
     * @param consumer, where {@link T} is a target object and {@link String} value by key from
     *     source map
     */
    public TargetHandler<T> map(String key, BiConsumer<T, String> consumer) {
      mapperMap.put(
          key,
          (t, v) -> {
            consumer.accept(t, v);
            return null;
          });
      return this;
    }

    /**
     * Maps the {@link Term} to a field in a target model, can process list of {@link String} issues
     *
     * @param key as a {@link Term} for source map
     * @param function, where {@link T} is a target object, {@link String} value by key from source
     *     map and {@link List<String>} list of issues
     */
    public TargetHandler<T> map(Term key, BiFunction<T, String, List<String>> function) {
      return map(key.qualifiedName(), function);
    }

    /**
     * Maps the {@link String} to a field in a target model, can process list of {@link String}
     * issues
     *
     * @param key as a {@link String} for source map
     * @param function, where {@link T} is a target object, {@link String} value by key from source
     *     map and {@link List<String>} list of issues
     */
    public TargetHandler<T> map(String key, BiFunction<T, String, List<String>> function) {
      mapperMap.put(key, function);
      return this;
    }

    public TargetHandler<T> map(Map<String, BiFunction<T, String, List<String>>> mapperMap) {
      this.mapperMap.putAll(mapperMap);
      return this;
    }

    /**
     * Maps the {@link Term} to a field in a target model, can process single {@link String} issue
     *
     * @param key as a {@link Term} for source map
     * @param function, where {@link T} is a target object, {@link String} value by key from source
     *     map and {@link List<String>} list of issues
     */
    public TargetHandler<T> mapOne(Term key, BiFunction<T, String, String> function) {
      return mapOne(key.qualifiedName(), function);
    }

    /**
     * Maps the {@link String} to a field in a target model, can process single {@link String} issue
     *
     * @param key as a {@link String} for source map
     * @param function, where {@link T} is a target object, {@link String} value by key from source
     *     map and {@link List<String>} list of issues
     */
    public TargetHandler<T> mapOne(String key, BiFunction<T, String, String> function) {
      mapperMap.put(
          key,
          (t, v) -> {
            String r = function.apply(t, v);
            return Strings.isNullOrEmpty(r)
                ? Collections.emptyList()
                : Collections.singletonList(r);
          });
      return this;
    }

    /**
     * Post mapper, if you want to process result {@link T} after main mappers, can process list of
     * {@link String} issue
     *
     * @param function, where {@link T} is a target object and {@link List<String>} list of issues
     */
    public TargetHandler<T> postMap(Function<T, List<String>> function) {
      postMapperSet.add(function);
      return this;
    }

    /**
     * Post mapper, if you want to process result {@link T} after main mappers, can't process an
     * issue
     *
     * @param consumer, where {@link T} is a target object
     */
    public TargetHandler<T> postMap(Consumer<T> consumer) {
      return postMap(
          t -> {
            consumer.accept(t);
            return Collections.emptyList();
          });
    }

    /**
     * Post mapper, if you want to process result {@link T} after main mappers, can process single
     * {@link String} issue
     *
     * @param function, where {@link T} is a target object and {@link List<String>} list of issues
     */
    public TargetHandler<T> postMapOne(Function<T, String> function) {
      return postMap(
          t -> {
            String r = function.apply(t);
            return Strings.isNullOrEmpty(r)
                ? Collections.emptyList()
                : Collections.singletonList(r);
          });
    }

    /**
     * Skips whole record if validator was triggered
     *
     * <pre>{@code
     * private static Optional<String> checkLink(Image i) {
     *   return i.getReferences() == null ? Optional.of("INVALID_LINK") : Optional.empty();
     * }
     * }</pre>
     *
     * @param validator function where {@link T} is a target object and Optional<String> if is an
     *     issue, or Optional.empty() if validation is OK
     */
    public TargetHandler<T> skipIf(Function<T, Optional<String>> validator) {
      validatorSet.add(validator);
      return this;
    }

    /**
     * @param extensions a list of maps as source of data
     * @return result of conversion
     */
    public Result<T> convert(List<Map<String, String>> extensions) {
      List<T> result = new ArrayList<>();
      Set<String> issues = new TreeSet<>();

      // Tries to get an extension from map by the name
      Optional.ofNullable(extensions)
          .filter(e -> !e.isEmpty())
          .ifPresent(
              listExt ->

                  // Process list of extensions
                  listExt.forEach(
                      ext -> {

                        // Creates the new target object
                        T t = supplier.get();

                        // Calls sequence of mappers
                        mapperMap.forEach(
                            (term, fn) -> {
                              if (ext.containsKey(term)) {
                                Optional.ofNullable(fn)
                                    .map(c -> fn.apply(t, ext.get(term)))
                                    .ifPresent(issues::addAll);
                              }
                            });

                        // Calls sequence of post mappers
                        postMapperSet.forEach(fn -> issues.addAll(fn.apply(t)));

                        // Calls sequence of validators
                        Optional<String> first =
                            validatorSet.stream()
                                .map(x -> x.apply(t))
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .findFirst();

                        // Collects issues
                        first.ifPresent(issues::add);

                        // Collects the result
                        if (!first.isPresent()) {
                          result.add(t);
                        }
                      }));

      return new Result<>(result, issues);
    }

    /**
     * @param extension a map as source of data
     * @return result of conversion
     */
    public Result<T> convert(Map<String, String> extension) {
      List<Map<String, String>> extensions = Collections.singletonList(extension);
      return convert(extensions);
    }

    /**
     * @param record an ExtendedRecord instance as source of data
     * @return result of conversion
     */
    public Result<T> convert(ExtendedRecord record) {
      List<Map<String, String>> extensions = record.getExtensions().get(extension);
      return convert(extensions);
    }
  }

  @AllArgsConstructor
  public static class Result<T> {

    private final List<T> items;
    private final Set<String> issues;

    /** @return Multi values result */
    public List<T> getList() {
      return items;
    }

    /** @return Single value result */
    public Optional<T> get() {
      return items.isEmpty() ? Optional.empty() : Optional.of(items.get(0));
    }

    /** @return Issues as set of values */
    public Set<String> getIssues() {
      return issues;
    }

    /** @return Issues as list of values */
    public List<String> getIssuesAsList() {
      return new ArrayList<>(issues);
    }
  }
}
