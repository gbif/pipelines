/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.tasks.modes;

import org.gbif.pipelines.tasks.PipelinesCallback;

/**
 * Factory for resolving internal {@link CallbackMode} implementations from public {@link
 * CallbackModeType} values.
 *
 * <p>{@link CallbackModeType} is exposed through the {@link PipelinesCallback} builder, while
 * {@link CallbackMode} implementations remain package-private implementation details.
 */
public final class CallbackModes {

  private CallbackModes() {}

  /**
   * Returns the callback mode implementation for the supplied type.
   *
   * <p>Unknown or {@code null} values are treated as regular pipeline mode by default. This keeps
   * the default behavior compatible with normal ingestion callbacks.
   */
  public static CallbackMode from(CallbackModeType type) {
    return type == CallbackModeType.VALIDATOR ? CallbackMode.validator() : CallbackMode.pipelines();
  }
}
