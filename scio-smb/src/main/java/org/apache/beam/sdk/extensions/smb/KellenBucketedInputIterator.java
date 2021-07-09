package org.apache.beam.sdk.extensions.smb;

import java.util.Iterator;
import java.util.Optional;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

class KellenBucketedInputIterator<K, V> {
  public final TupleTag<?> tupleTag;
  public final boolean emitByDefault;

  private final KeyGroupIterator<byte[], V> iter;
  final SortedBucketSource.Predicate<V> predicate;
  private Optional<KV<byte[], Iterator<V>>> head;

  public KellenBucketedInputIterator(
      SortedBucketSource.BucketedInput<K, V>  source,
      int bucketId,
      int parallelism,
      PipelineOptions options
  ) {
    this.predicate = source.predicate;
    this.tupleTag = source.getTupleTag();
    this.iter = source.createIterator(bucketId, parallelism, options);

    int numBuckets = source.getOrComputeMetadata().getCanonicalMetadata().getNumBuckets();
    // The canonical # buckets for this source. If # buckets >= the parallelism of the job,
    // we know that the key doesn't need to be re-hashed as it's already in the right bucket.
    this.emitByDefault = numBuckets >= parallelism;

    advance();
  }

  public boolean hasPredicate() {
    return this.predicate != null;
  }

  public byte[] currentKey() {
    return head.get().getKey();
  }

  public Iterator<V> currentValue() {
    return head.get().getValue();
  }

  public boolean isExhausted() {
    return !head.isPresent();
  }

  public void advance() {
    if(iter.hasNext()) {
      head = Optional.of(iter.next());
    } else {
      head = Optional.empty();
    }
  }
}
