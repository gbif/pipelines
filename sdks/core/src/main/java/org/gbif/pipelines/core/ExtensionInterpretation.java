package org.gbif.pipelines.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
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

    public TargetHandler<T> mapIssue(Term key, BiFunction<T, String, String> function) {
      mapperMap.put(key.qualifiedName(), (t, v) -> {
        String r = function.apply(t, v);
        return Strings.isNullOrEmpty(r) ? Collections.emptyList() : Collections.singletonList(r);
      });
      return this;
    }

    public TargetHandler<T> mapIssues(Term key, BiFunction<T, String, List<String>> function) {
      mapperMap.put(key.qualifiedName(), function);
      return this;
    }

    public TargetHandler<T> mapIssues(String key, BiFunction<T, String, List<String>> function) {
      mapperMap.put(key, function);
      return this;
    }

    public TargetHandler<T> mapIssues(Map<String, BiFunction<T, String, List<String>>> mapperMap) {
      this.mapperMap.putAll(mapperMap);
      return this;
    }

    public Result<T> convert(ExtendedRecord record) {
      List<T> result = new ArrayList<>();
      List<String> issues = new ArrayList<>();

      List<Map<String, String>> exts = record.getExtensions().get(extenstion);
      Optional.ofNullable(exts).filter(e -> !e.isEmpty()).ifPresent(listExt ->
          listExt.forEach(ext -> {
            T t = supplier.get();
            ext.forEach((k, v) -> {
              BiFunction<T, String, List<String>> fn = mapperMap.get(k);
              Optional.ofNullable(fn).map(c -> fn.apply(t, v)).ifPresent(issues::addAll);
            });
            result.add(t);
          }));

      return new Result<>(result, issues);
    }

  }

  public class Result<T> {

    private final List<T> result;
    private final List<String> issues;

    public Result(List<T> result, List<String> issues) {
      this.result = result;
      this.issues = issues;
    }

    public List<T> getList() {
      return result;
    }

    public Optional<T> get() {
      return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0));
    }

    public List<String> getIssues() {
      return issues;
    }
  }

}
