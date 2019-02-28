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

      Optional.ofNullable(extensions)
          .filter(e -> !e.isEmpty())
          .ifPresent(listExt ->
              listExt.forEach(ext -> {
                //
                T t = supplier.get();

                //
                ext.forEach((k, v) -> {
                  BiFunction<T, String, List<String>> fn = mapperMap.get(k);
                  Optional.ofNullable(fn).map(c -> fn.apply(t, v)).ifPresent(issues::addAll);
                });

                //
                postMapperSet.forEach(fn -> issues.addAll(fn.apply(t)));

                //
                Optional<String> first = checkerSet.stream()
                    .map(x -> x.apply(t))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst();

                first.ifPresent(issues::add);

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
