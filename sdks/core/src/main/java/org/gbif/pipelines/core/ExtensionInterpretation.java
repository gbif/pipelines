package org.gbif.pipelines.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * The class is designed to simplify extension interpretation process:
 *
 * <pre>
 *   1) Set extension name {@link ExtensionInterpretation#extenstion(Extension)}
 *   or {@link ExtensionInterpretation#extenstion(String)}, basically, it will filter try to get from a map by name
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
 *  private static final TargetHandler<Image> HANDLER =
 *      ExtensionInterpretation.extenstion(Extension.IMAGE)
 *        .to(Image::new)
 *        .map(DcTerm.identifier, ImageInterpreter::parseAndsetIdentifier)
 *        .mapOne("http://www.w3.org/2003/01/geo/wgs84_pos#longitude", ImageInterpreter::parseAndSetLongitude)
 *        .postMap(ImageInterpreter::parseAndSetLatLng)
 *        .skipIf(ImageInterpreter::checkLinks);
 *
 *  Result<Image> result = HANDLER.convert(er);
 *  result.getList();
 *  result.getIssuesAsList();
 * }</pre>
 *
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.AmplificationInterpreter}
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.AudubonInterpreter}
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.ImageInterpreter}
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.MeasurementOrFactInterpreter}
 * <p>Example: {@link org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter}
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ExtensionInterpretation {

  private final String extenstion;

  public static ExtensionInterpretation extenstion(String extenstion) {
    return new ExtensionInterpretation(extenstion);
  }

  public static ExtensionInterpretation extenstion(Extension extenstion) {
    return new ExtensionInterpretation(extenstion.getRowType());
  }

  public <T> TargetHandler<T> to(Supplier<T> supplier) {
    return new TargetHandler<>(supplier);
  }

  public class TargetHandler<T> {

    private final Supplier<T> supplier;

    private Map<String, BiFunction<T, String, List<String>>> mapperMap = new HashMap<>();

    private Set<Function<T, List<String>>> postMapperSet = new HashSet<>();

    private Set<Function<T, Optional<String>>> checkerSet = new HashSet<>();

    private TargetHandler(Supplier<T> supplier) {
      this.supplier = supplier;
    }

    public TargetHandler<T> map(Term key, BiConsumer<T, String> consumer) {
      return map(key.qualifiedName(), consumer);
    }

    public TargetHandler<T> map(String key, BiConsumer<T, String> consumer) {
      mapperMap.put(key, (t, v) -> {
        consumer.accept(t, v);
        return null;
      });
      return this;
    }

    public TargetHandler<T> map(Term key, BiFunction<T, String, List<String>> function) {
      return map(key.qualifiedName(), function);
    }

    public TargetHandler<T> map(String key, BiFunction<T, String, List<String>> function) {
      mapperMap.put(key, function);
      return this;
    }

    public TargetHandler<T> map(Map<String, BiFunction<T, String, List<String>>> mapperMap) {
      this.mapperMap.putAll(mapperMap);
      return this;
    }

    public TargetHandler<T> mapOne(Term key, BiFunction<T, String, String> function) {
      return mapOne(key.qualifiedName(), function);
    }

    public TargetHandler<T> mapOne(String key, BiFunction<T, String, String> function) {
      mapperMap.put(key, (t, v) -> {
        String r = function.apply(t, v);
        return Strings.isNullOrEmpty(r) ? Collections.emptyList() : Collections.singletonList(r);
      });
      return this;
    }

    public TargetHandler<T> postMap(Function<T, List<String>> function) {
      postMapperSet.add(function);
      return this;
    }

    public TargetHandler<T> postMap(Consumer<T> consumer) {
      return postMap(t -> {
        consumer.accept(t);
        return null;
      });
    }

    public TargetHandler<T> postMapOne(Function<T, String> function) {
      return postMap(t -> {
        String r = function.apply(t);
        return Strings.isNullOrEmpty(r) ? Collections.emptyList() : Collections.singletonList(r);
      });
    }

    public TargetHandler<T> skipIf(Function<T, Optional<String>> checker) {
      checkerSet.add(checker);
      return this;
    }

    public Result<T> convert(List<Map<String, String>> extensions) {
      List<T> result = new ArrayList<>();
      Set<String> issues = new HashSet<>();

      // Tries to get an extension from map by the name
      Optional.ofNullable(extensions)
          .filter(e -> !e.isEmpty())
          .ifPresent(listExt ->

              // Process list of extensions
              listExt.forEach(ext -> {

                // Creates the new target object
                T t = supplier.get();

                // Calls sequence of mappers
                ext.forEach((k, v) -> {
                  BiFunction<T, String, List<String>> fn = mapperMap.get(k);
                  Optional.ofNullable(fn).map(c -> fn.apply(t, v)).ifPresent(issues::addAll);
                });

                // Calls sequence of post mappers
                postMapperSet.forEach(fn -> issues.addAll(fn.apply(t)));

                // Calls sequence of validators
                Optional<String> first = checkerSet.stream()
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

    public Result<T> convert(Map<String, String> extension) {
      List<Map<String, String>> exts = Collections.singletonList(extension);
      return convert(exts);
    }

    public Result<T> convert(ExtendedRecord record) {
      List<Map<String, String>> exts = record.getExtensions().get(extenstion);
      return convert(exts);
    }

  }

  @AllArgsConstructor
  public static class Result<T> {

    private final List<T> items;
    private final Set<String> issues;

    public List<T> getList() {
      return items;
    }

    public Optional<T> get() {
      return items.isEmpty() ? Optional.empty() : Optional.of(items.get(0));
    }

    public Set<String> getIssues() {
      return issues;
    }

    public List<String> getIssuesAsList() {
      return new ArrayList<>(issues);
    }
  }

}
