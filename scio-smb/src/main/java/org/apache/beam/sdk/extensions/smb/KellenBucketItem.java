package org.apache.beam.sdk.extensions.smb;

import java.io.Serializable;
import java.util.Objects;

public class KellenBucketItem implements Serializable {
  final int bucketOffsetId;
  final int effectiveParallelism;

  public KellenBucketItem(Integer bucketOffsetId, Integer effectiveParallelism) {
    this.bucketOffsetId = bucketOffsetId;
    this.effectiveParallelism = effectiveParallelism;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KellenBucketItem that = (KellenBucketItem) o;
    return Objects.equals(bucketOffsetId, that.bucketOffsetId)
           && Objects.equals(effectiveParallelism, that.effectiveParallelism);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucketOffsetId, effectiveParallelism);
  }
}
