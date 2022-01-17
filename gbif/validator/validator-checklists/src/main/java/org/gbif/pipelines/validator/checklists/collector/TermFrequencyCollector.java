package org.gbif.pipelines.validator.checklists.collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.validator.checklists.model.NormalizedNameUsageData;
import org.gbif.pipelines.validator.checklists.model.NormalizedTermMapUsageData;
import org.gbif.validator.api.Metrics;

@Data
@NoArgsConstructor
public class TermFrequencyCollector {

  @Data
  public static class TermFrequency {

    public static final TermFrequency EMPTY = new TermFrequency();

    private final Map<Term, Long> termsFrequency;

    public TermFrequency() {
      termsFrequency = new HashMap<>();
    }

    private TermFrequency(Map<Term, Long> termsFrequency) {
      this.termsFrequency = termsFrequency;
    }

    public Long inc(Term term) {
      return add(term, 1L);
    }

    public Long add(Term term, Long value) {
      return termsFrequency.compute(
          term,
          (k, v) -> {
            if (v == null && value == null) {
              return 0L;
            }
            if (v == null) {
              return value;
            }
            if (value == null) {
              return v;
            }
            return v + value;
          });
    }

    public TermFrequency inc(Map<Term, ?> termsMap) {
      if (termsMap != null) {
        termsMap.forEach(
            (k, v) -> {
              if (v != null) {
                inc(k);
              } else {
                add(k, 0L);
              }
            });
      }
      return this;
    }

    public TermFrequency add(Map<Term, Long> termsFrequency) {
      if (termsFrequency != null) {
        termsFrequency.forEach(
            (k, v) -> {
              if (v != null) {
                add(k, v);
              }
            });
      }
      return this;
    }

    public TermFrequency add(TermFrequency termFrequency) {
      return add(termFrequency.termsFrequency);
    }

    public <T> TermFrequency inc(List<Map<Term, T>> termsMap) {
      if (termsMap != null) {
        termsMap.forEach(this::inc);
      }
      return this;
    }

    public TermFrequency add(List<Map<Term, Long>> termsFrequencies) {
      if (termsFrequencies != null) {
        termsFrequencies.forEach(this::add);
      }
      return this;
    }

    public Long getFrequency(Term term) {
      return Optional.ofNullable(termsFrequency.get(term)).orElse(0L);
    }

    public static <T> TermFrequency of(List<Map<Term, T>> termsMap) {
      TermFrequency termFrequency = new TermFrequency();
      return termFrequency.inc(termsMap);
    }

    public static TermFrequency combine(
        TermFrequency termFrequency1, TermFrequency termFrequency2) {
      TermFrequency result = new TermFrequency(termFrequency1.termsFrequency);
      result.add(termFrequency2);
      return result;
    }
  }

  private final TermFrequency verbatimTermsFrequency = new TermFrequency();

  private final Map<Extension, TermFrequency> verbatimExtensionsTermsFrequency = new HashMap<>();

  private final Map<Extension, Long> verbatimExtensionsRowCount = new HashMap<>();

  private final TermFrequency interpretedTermsFrequency = new TermFrequency();

  private final Map<Extension, TermFrequency> interpretedExtensionsTermsFrequency = new HashMap<>();

  private final Map<Extension, Long> interpretedExtensionsRowCount = new HashMap<>();

  public TermFrequencyCollector verbatimTerms(Map<Term, ?> termsMap) {
    verbatimTermsFrequency.inc(termsMap);
    return this;
  }

  public <T> TermFrequencyCollector verbatimExtensions(
      Map<Extension, List<Map<Term, T>>> verbatimExtensionsMap) {
    if (verbatimExtensionsMap != null) {
      verbatimExtensionsMap.forEach(
          (k, v) -> {
            verbatimExtensionsTermsFrequency.put(k, TermFrequency.of(v));
            verbatimExtensionsRowCount.compute(
                k, (ext, count) -> Optional.ofNullable(count).orElse(0L) + (long) v.size());
          });
    }
    return this;
  }

  public <T> TermFrequencyCollector interpretedExtensions(
      Map<Extension, List<Map<Term, T>>> interpretedExtensionsMap) {
    if (interpretedExtensionsMap != null) {
      interpretedExtensionsMap.forEach(
          (k, v) -> {
            interpretedExtensionsTermsFrequency.put(k, TermFrequency.of(v));
            interpretedExtensionsRowCount.compute(
                k, (ext, count) -> Optional.ofNullable(count).orElse(0L) + (long) v.size());
          });
    }
    return this;
  }

  public Long getVerbatimExtensionRowCount(Extension extension) {
    return Optional.ofNullable(verbatimExtensionsRowCount.get(extension)).orElse(0L);
  }

  public Long getInterpretedExtensionRowCount(Extension extension) {
    return Optional.ofNullable(interpretedExtensionsRowCount.get(extension)).orElse(0L);
  }

  public TermFrequencyCollector interpretedTerms(Map<Term, ?> termsMap) {
    interpretedTermsFrequency.inc(termsMap);
    return this;
  }

  /** Concat the key sets of both maps. */
  private Set<Term> termsUnion(Map<Term, ?> termsMap1, Map<Term, ?> termsMap2) {
    return Stream.concat(
            Optional.ofNullable(termsMap1).map(m -> m.keySet().stream()).orElse(Stream.empty()),
            Optional.ofNullable(termsMap2).map(m -> m.keySet().stream()).orElse(Stream.empty()))
        .collect(Collectors.toSet());
  }

  /** Concat the key sets of both maps. */
  private Set<Extension> extensionsUnion(Map<Extension, ?> termsMap1, Map<Extension, ?> termsMap2) {
    return Stream.concat(
            Optional.ofNullable(termsMap1).map(m -> m.keySet().stream()).orElse(Stream.empty()),
            Optional.ofNullable(termsMap2).map(m -> m.keySet().stream()).orElse(Stream.empty()))
        .collect(Collectors.toSet());
  }

  private Metrics.TermInfo toTermInfo(Term term) {
    return Metrics.TermInfo.builder()
        .term(term.qualifiedName())
        .rawIndexed(verbatimTermsFrequency.getFrequency(term))
        .interpretedIndexed(interpretedTermsFrequency.getFrequency(term))
        .build();
  }

  private Long getVerbatimExtensionFrequency(Extension extension, Term term) {
    return Optional.ofNullable(verbatimExtensionsTermsFrequency.get(extension))
        .map(tf -> tf.getFrequency(term))
        .orElse(0L);
  }

  private Long getInterpretedExtensionFrequency(Extension extension, Term term) {
    return Optional.ofNullable(interpretedExtensionsTermsFrequency.get(extension))
        .map(tf -> tf.getFrequency(term))
        .orElse(0L);
  }

  public Metrics.TermInfo toExtensionTermInfo(Extension extension, Term term) {
    return Metrics.TermInfo.builder()
        .term(term.qualifiedName())
        .rawIndexed(getVerbatimExtensionFrequency(extension, term))
        .interpretedIndexed(getInterpretedExtensionFrequency(extension, term))
        .build();
  }

  public TermFrequency getVerbatimExtensionFrequency(Extension extension) {
    return Optional.ofNullable(verbatimExtensionsTermsFrequency.get(extension))
        .orElse(TermFrequency.EMPTY);
  }

  public TermFrequency getInterpretedExtensionFrequency(Extension extension) {
    return Optional.ofNullable(interpretedExtensionsTermsFrequency.get(extension))
        .orElse(TermFrequency.EMPTY);
  }

  public List<Metrics.TermInfo> toExtensionTermInfo(Extension extension) {
    return termsUnion(
            getVerbatimExtensionFrequency(extension).getTermsFrequency(),
            getInterpretedExtensionFrequency(extension).getTermsFrequency())
        .stream()
        .map(term -> toExtensionTermInfo(extension, term))
        .collect(Collectors.toList());
  }

  public List<Metrics.TermInfo> toTermsInfo() {
    return termsUnion(
            verbatimTermsFrequency.getTermsFrequency(),
            interpretedTermsFrequency.getTermsFrequency())
        .stream()
        .map(this::toTermInfo)
        .collect(Collectors.toList());
  }

  private TermFrequencyCollector addVerbatimExtensionFrequency(
      Map<Extension, TermFrequency> extensionTermFrequency) {
    return addExtensionFrequency(extensionTermFrequency, verbatimExtensionsTermsFrequency);
  }

  private TermFrequencyCollector addInterpretedExtensionFrequency(
      Map<Extension, TermFrequency> extensionTermFrequency) {
    return addExtensionFrequency(extensionTermFrequency, interpretedExtensionsTermsFrequency);
  }

  private TermFrequencyCollector addVerbatimExtensionsCount(Map<Extension, Long> extensionsCount) {
    return addExtensionsCount(verbatimExtensionsRowCount, extensionsCount);
  }

  private TermFrequencyCollector addInterpretedExtensionsCount(
      Map<Extension, Long> extensionsCount) {
    return addExtensionsCount(interpretedExtensionsRowCount, extensionsCount);
  }

  private TermFrequencyCollector addExtensionsCount(
      Map<Extension, Long> fromExtensionsCount, Map<Extension, Long> toExtensionsCount) {
    extensionsUnion(fromExtensionsCount, toExtensionsCount)
        .forEach(
            extension ->
                fromExtensionsCount.compute(
                    extension,
                    (k, v) ->
                        Optional.ofNullable(v).orElse(0L)
                            + Optional.ofNullable(toExtensionsCount.get(extension)).orElse(0L)));
    return this;
  }

  private TermFrequencyCollector addExtensionFrequency(
      Map<Extension, TermFrequency> from, Map<Extension, TermFrequency> to) {
    extensionsUnion(to, from)
        .forEach(
            extension ->
                Optional.ofNullable(from.get(extension))
                    .ifPresent(
                        fromTf -> {
                          if (to.get(extension) != null) {
                            to.get(extension).add(fromTf);
                          } else {
                            to.put(extension, fromTf);
                          }
                        }));
    return this;
  }

  public TermFrequencyCollector add(TermFrequencyCollector collector) {
    interpretedTermsFrequency.add(collector.interpretedTermsFrequency);
    verbatimTermsFrequency.add(collector.verbatimTermsFrequency);
    return addInterpretedExtensionFrequency(collector.interpretedExtensionsTermsFrequency)
        .addVerbatimExtensionFrequency(collector.verbatimExtensionsTermsFrequency)
        .addInterpretedExtensionsCount(collector.interpretedExtensionsRowCount)
        .addVerbatimExtensionsCount(collector.verbatimExtensionsRowCount);
  }

  public static TermFrequencyCollector of(NormalizedNameUsageData normalizedNameUsageData) {
    NormalizedTermMapUsageData termMapUsageData =
        NormalizedTermMapUsageData.of(normalizedNameUsageData);
    return new TermFrequencyCollector()
        .verbatimTerms(termMapUsageData.getVerbatimNameUsage())
        .interpretedTerms(termMapUsageData.getInterpretedNameUsage())
        .verbatimExtensions(termMapUsageData.getVerbatimExtensions())
        .interpretedExtensions(termMapUsageData.getInterpretedExtensions());
  }
}
