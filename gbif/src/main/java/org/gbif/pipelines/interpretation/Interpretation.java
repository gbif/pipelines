package org.gbif.pipelines.interpretation;

import org.gbif.pipelines.core.functions.interpretation.error.*;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A container object of interpretation result that can be combined with the result of other interpretations.
 *
 * @param <T> type of context element use as an input for interpretation
 */
public class Interpretation<T> implements Serializable {

    //Element to be interpreted
    private final T value;
    //Stores the transformations and operations applied during an interpretation
    private final List<Trace<LineageType>> lineage;
    //Stores the validations applied during an interpretation
    private final List<Trace<IssueType>> validations;

    /**
     * Full constructor.
     */
    private Interpretation(T value, List<Trace<IssueType>> validations, List<Trace<LineageType>> lineage) {
        this.value = value;
        this.validations = validations;
        this.lineage = lineage;
    }

    /**
     * Creates a interpretation of a value.
     */
    public static <U> Interpretation<U> of(U value) {
        return new Interpretation<>(value, new ArrayList<>(), new ArrayList<>());
    }

    /**
     * Adds a validation to the applied interpretation.
     */
    public Interpretation<T> withValidation(List<Trace<IssueType>> validations) {
        this.validations.addAll(validations);
        return this;
    }

    /**
     * Adds a validation to the applied interpretation.
     */
    public Interpretation<T> withValidation(String fieldName, List<Issue> validations) {
        validations.forEach((validation) -> this.validations.add(Trace.of(fieldName, validation.getIssueType(), validation.getRemark().toString())));
        return this;
    }

    /**
     * Adds a lineage trace to the interpretation operation.
     */
    public Interpretation<T> withLineage(List<Trace<LineageType>> lineages) {
        this.lineage.addAll(lineages);
        return this;
    }

    /**
     * Adds a lineage trace to the interpretation operation.
     */
    public Interpretation<T> withLineage(String fieldName, List<Lineage> lineages) {
        lineages.forEach((lineage) -> this.lineage.add(Trace.of(fieldName, lineage.getLineageType(), lineage.getRemark().toString())));
        return this;
    }

    public <U> Interpretation<U> using(Function<? super T, Interpretation<U>> mapper) {
        Interpretation<U> interpretation = mapper.apply(value);

        List<Trace<LineageType>> newLineage = new ArrayList<>(lineage);
        newLineage.addAll(interpretation.lineage);

        List<Trace<IssueType>> newValidations = new ArrayList<>(validations);
        newValidations.addAll(interpretation.validations);

        return new Interpretation<>(interpretation.value, newValidations, newLineage);
    }

    /**
     * Consumes all traces in the validation.
     */
    public void forEachValidation(Consumer<Trace<IssueType>> traceConsumer) {
        validations.forEach(traceConsumer);
    }

    /**
     * Consumes all traces in the lineage.
     */
    public void forEachLineage(Consumer<Trace<LineageType>> traceConsumer) {
        lineage.forEach(traceConsumer);
    }

    public IssueLineageRecord getIssueLineageRecord(CharSequence occurenceId) {
        Map<CharSequence, List<Issue>> fieldIssueMap = new HashMap<>();
        Map<CharSequence, List<Lineage>> fieldLineageMap = new HashMap<>();

        this.forEachValidation((issueTrace) -> {
            final Issue build = Issue.newBuilder().setRemark(issueTrace.getRemark()).setIssueType(issueTrace.context).build();
            if (fieldIssueMap.containsKey(issueTrace.fieldName))
                fieldIssueMap.get(issueTrace.fieldName).add(build);
            fieldIssueMap.putIfAbsent(issueTrace.fieldName, Arrays.asList(build));
        });
        this.forEachLineage((lineageTrace) -> {
            final Lineage build = Lineage.newBuilder().setRemark(lineageTrace.getRemark()).setLineageType(lineageTrace.context).build();
            if (fieldLineageMap.containsKey(lineageTrace.fieldName))
                fieldLineageMap.get(lineageTrace.fieldName).add(build);
            fieldLineageMap.putIfAbsent(lineageTrace.fieldName, Arrays.asList(build));
        });
        return IssueLineageRecord.newBuilder().setFieldLineageMap(fieldLineageMap).setFieldIssueMap(fieldIssueMap).setOccurenceId(occurenceId).build();

    }

    /**
     * Container class for an element that needs to be tracked during an interpretation.
     *
     * @param <T> type of element to be tracked
     */
    public static class Trace<T> implements Serializable {

        private final String fieldName;
        //What this class is tracing
        private final T context;

        //Observation about a trace event
        private final String remark;

        /**
         * Creates an instance of traceable element.
         */
        private Trace(String fieldName, T context, String remark) {
            this.fieldName = fieldName;
            this.context = context;
            this.remark = remark;
        }

        /**
         * Factory method to create a instance of trace object using a context element.
         */
        public static <U> Trace<U> of(String fieldName, U context) {
            return Trace.of(fieldName, context, null);
        }

        /**
         * Factory method to create a full instance of a trace object.
         */
        public static <U> Trace<U> of(String fieldName, U context, String remark) {
            return new Trace<>(fieldName, context, remark);
        }

        /**
         * field name of element being traced
         *
         * @return
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * @return the element being traced
         */
        public T getContext() {
            return context;
        }

        /**
         * @return any comment or observation about the traced element
         */
        public String getRemark() {
            return remark;
        }
    }
}
