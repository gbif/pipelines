package org.gbif.validator.it.mocks;

import java.util.UUID;
import org.gbif.api.model.ChallengeCode;
import org.gbif.registry.surety.ChallengeCodeManager;

public class ChallengeCodeManagerMock implements ChallengeCodeManager<Integer> {

  @Override
  public boolean isValidChallengeCode(Integer o, UUID uuid, String s) {
    return true;
  }

  @Override
  public boolean hasChallengeCode(Integer o) {
    return false;
  }

  @Override
  public ChallengeCode create(Integer o) {
    return ChallengeCode.newRandom();
  }

  @Override
  public ChallengeCode create(Integer o, String s) {
    return ChallengeCode.newRandom();
  }

  @Override
  public void remove(Integer o) {}
}
