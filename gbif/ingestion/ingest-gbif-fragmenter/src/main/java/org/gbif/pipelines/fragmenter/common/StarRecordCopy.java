package org.gbif.pipelines.fragmenter.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.Term;
import org.jetbrains.annotations.NotNull;

/** Helps to avoid the issue with StarRecordImpl iterator which returns the same object pointer */
public class StarRecordCopy implements StarRecord {

  private final Record core;
  private final Map<Term, List<Record>> extensions;

  private StarRecordCopy(StarRecord record) {
    this.core = record.core();
    this.extensions = new HashMap<>(record.size());
    record.extensions().forEach((key, value) -> extensions.put(key, new ArrayList<>(value)));
  }

  public static StarRecord create(StarRecord record) {
    return new StarRecordCopy(record);
  }

  @Override
  public Record core() {
    return core;
  }

  @Override
  public boolean hasExtension(Term term) {
    return extensions.containsKey(term);
  }

  @Override
  public Map<Term, List<Record>> extensions() {
    return extensions;
  }

  @Override
  public List<Record> extension(Term term) {
    return extensions.get(term);
  }

  @Override
  public Set<Term> rowTypes() {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @NotNull
  @Override
  public Iterator<Record> iterator() {
    throw new UnsupportedOperationException("The method is not implemented!");
  }
}
