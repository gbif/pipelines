package org.gbif.pipelines.core.interpreters.model;

import org.jetbrains.annotations.NotNull;

public interface EventDate {
   void setGte(@NotNull String s);
   void setLte(@NotNull String s);
   void setInterval(String string);
}
