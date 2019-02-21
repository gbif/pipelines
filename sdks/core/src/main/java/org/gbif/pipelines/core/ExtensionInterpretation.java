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
import java.util.function.Function;
import java.util.function.Supplier;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import com.google.common.base.Strings;

public class ExtensionInterpretation {

  private final String extenstion;

  private ExtensionInterpretation(String extenstion) {
    this.extenstion = extenstion;
  }

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

    private Function<T, Optional<String>> checker;

    private Map<String, BiFunction<T, String, List<String>>> mapperMap = new HashMap<>();

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

    public TargetHandler<T> mapFn(Term key, BiFunction<T, String, String> function) {
      mapperMap.put(key.qualifiedName(), (t, v) -> {
        String r = function.apply(t, v);
        return Strings.isNullOrEmpty(r) ? Collections.emptyList() : Collections.singletonList(r);
      });
      return this;
    }

    public TargetHandler<T> map(Term key, BiFunction<T, String, List<String>> function) {
      mapperMap.put(key.qualifiedName(), function);
      return this;
    }

    public TargetHandler<T> map(String key, BiFunction<T, String, List<String>> function) {
      mapperMap.put(key, function);
      return this;
    }

    public TargetHandler<T> map(Map<String, BiFunction<T, String, List<String>>> mapperMap) {
      this.mapperMap.putAll(mapperMap);
      return this;
    }

    public TargetHandler<T> skipIf(Function<T, Optional<String>> checker) {
      this.checker = checker;
      return this;
    }

    public Result<T> convert(ExtendedRecord record) {
      List<T> result = new ArrayList<>();
      Set<String> issues = new HashSet<>();

      List<Map<String, String>> exts = record.getExtensions().get(extenstion);
      Optional.ofNullable(exts).filter(e -> !e.isEmpty()).ifPresent(listExt ->
          listExt.forEach(ext -> {
            //
            T t = supplier.get();
            //
            ext.forEach((k, v) -> {
              BiFunction<T, String, List<String>> fn = mapperMap.get(k);
              Optional.ofNullable(fn).map(c -> fn.apply(t, v)).ifPresent(issues::addAll);
            });
            //
            Optional<String> skipIssue = checker.apply(t);
            skipIssue.ifPresent(issues::add);
            if (!skipIssue.isPresent()) {
              result.add(t);
            }

          }));

      return new Result<>(result, issues);
    }

  }

  public class Result<T> {

    private final List<T> items;
    private final Set<String> issues;

    public Result(List<T> items, Set<String> issues) {
      this.items = items;
      this.issues = issues;
    }

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
