package org.gbif.pipelines.fragmenter.common;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.Term;

import org.jetbrains.annotations.NotNull;

public class StarRecordCopy implements StarRecord {

  private final Record core;
  private final Map<Term, List<Record>> extensions;

  private StarRecordCopy(StarRecord record) {
    this.core = record.core();
    this.extensions = record.extensions();
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
    return false;
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
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  @NotNull
  @Override
  public Iterator<Record> iterator() {
    return null;
  }
}
