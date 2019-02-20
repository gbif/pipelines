package org.gbif.pipelines.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

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

  public SourceHandler from(ExtendedRecord record) {
    return new SourceHandler(record);
  }

  public class SourceHandler {

    private final ExtendedRecord record;

    private SourceHandler(ExtendedRecord record) {
      this.record = record;
    }

    public <T> TargetHandler<T> to(Supplier<T> supplier) {
      return new TargetHandler<>(supplier);
    }

    public class TargetHandler<T> {

      private final Supplier<T> supplier;

      private Map<String, BiConsumer<T, String>> mapperMap = new HashMap<>();

      private TargetHandler(Supplier<T> supplier) {
        this.supplier = supplier;
      }

      public TargetHandler<T> map(Term key, BiConsumer<T, String> consumer) {
        mapperMap.put(key.qualifiedName(), consumer);
        return this;
      }

      public TargetHandler<T> map(String key, BiConsumer<T, String> consumer) {
        mapperMap.put(key, consumer);
        return this;
      }

      public TargetHandler<T> map(Map<String, BiConsumer<T, String>> mapperMap) {
        this.mapperMap.putAll(mapperMap);
        return this;
      }

      public Result<T> convert() {
        List<T> result = new ArrayList<>();

        List<Map<String, String>> exts = record.getExtensions().get(extenstion);
        Optional.ofNullable(exts).filter(e -> !e.isEmpty()).ifPresent(listExt ->
            listExt.forEach(ext -> {
              T t = supplier.get();
              ext.forEach((k, v) -> {
                BiConsumer<T, String> consumer = mapperMap.get(k);
                Optional.ofNullable(consumer).ifPresent(c -> c.accept(t, v));
              });
              result.add(t);
            }));

        return new Result<>(result);
      }

    }

    public class Result<T> {

      private final List<T> result;

      public Result(List<T> result) {
        this.result = result;
      }

      public List<T> getList() {
        return result;
      }

      public Optional<T> get() {
        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0));
      }
    }

  }

}
