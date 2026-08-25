package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;

/**
 * Schema-resolved, ambiguity-checked, engine-neutral mapping between configuration and execution.
 */
public record CompiledMapping(
    String name,
    CoreType coreType,
    String coreSourceResource,
    Optional<CompiledTargetProducer> coreIdentity,
    List<CompiledTargetProducer> coreTargets,
    List<CompiledCoreFragment> coreFragments,
    List<CompiledTargetMerge> coreTargetMerges,
    List<CompiledExtension> extensions,
    List<MappingDecision> coreDecisions) {

  public CompiledMapping {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(coreType, "coreType");
    Objects.requireNonNull(coreSourceResource, "coreSourceResource");
    coreIdentity = coreIdentity == null ? Optional.empty() : coreIdentity;
    coreTargets = List.copyOf(coreTargets);
    coreFragments = List.copyOf(coreFragments);
    coreTargetMerges = List.copyOf(coreTargetMerges);
    extensions = List.copyOf(extensions);
    coreDecisions = List.copyOf(coreDecisions);
  }

  public List<MappingDecision> decisions() {
    List<MappingDecision> all = new ArrayList<>(coreDecisions);
    extensions.forEach(extension -> all.addAll(extension.decisions()));
    return List.copyOf(all);
  }
}
