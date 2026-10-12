package org.gbif.validator.orchestrator.workflow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.gbif.api.model.pipelines.StepType;

/** Immutable workflow dependencies and optional per-node join policies. */
public final class DagDefinition {

  public enum JoinPolicy {
    NON_ERROR,
    ALL_TERMINAL
  }

  public record Node(StepType type, Set<StepType> dependencies, JoinPolicy joinPolicy) {
    public Node {
      Objects.requireNonNull(type, "type");
      dependencies = Set.copyOf(Objects.requireNonNull(dependencies, "dependencies"));
      if (dependencies.contains(type)) {
        throw new IllegalArgumentException("Self dependency: " + type);
      }
    }

    public Node(StepType type, Set<StepType> dependencies) {
      this(type, dependencies, null);
    }
  }

  private final Map<StepType, Node> nodes;
  private final JoinPolicy defaultJoinPolicy;
  private final boolean parallel;

  public DagDefinition(List<Node> nodes, JoinPolicy defaultJoinPolicy, boolean parallel) {
    this.defaultJoinPolicy = Objects.requireNonNull(defaultJoinPolicy, "defaultJoinPolicy");
    this.parallel = parallel;

    Map<StepType, Node> byType = new EnumMap<>(StepType.class);
    for (Node node : nodes) {
      if (byType.putIfAbsent(node.type(), node) != null) {
        throw new IllegalArgumentException("Duplicate node: " + node.type());
      }
    }

    for (Node node : nodes) {
      for (StepType predecessor : node.dependencies()) {
        if (!byType.containsKey(predecessor)) {
          throw new IllegalArgumentException("Unknown dependency " + predecessor + " for " + node.type());
        }
      }
    }

    this.nodes = Map.copyOf(byType);
    Map<StepType, Boolean> visited = new HashMap<>();
    for (StepType type : byType.keySet()) {
      checkAcyclic(type, visited);
    }
  }

  private void checkAcyclic(StepType type, Map<StepType, Boolean> visited) {
    Boolean state = visited.get(type);
    if (Boolean.TRUE.equals(state)) {
      return;
    }
    if (Boolean.FALSE.equals(state)) {
      throw new IllegalArgumentException("Cycle involving " + type);
    }

    visited.put(type, false);
    for (StepType dependency : nodes.get(type).dependencies()) {
      checkAcyclic(dependency, visited);
    }
    visited.put(type, true);
  }

  public Node require(StepType type) {
    Node node = nodes.get(type);
    if (node == null) {
      throw new IllegalArgumentException("Unknown DAG node " + type);
    }
    return node;
  }

  public Set<StepType> stepTypes() {
    return nodes.keySet();
  }

  public JoinPolicy policy(Node node) {
    return node.joinPolicy() == null ? defaultJoinPolicy : node.joinPolicy();
  }

  public boolean parallel() {
    return parallel;
  }
}
