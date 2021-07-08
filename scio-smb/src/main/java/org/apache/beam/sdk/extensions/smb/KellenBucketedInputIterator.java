package org.apache.beam.sdk.extensions.smb;

import java.util.Iterator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

class KellenBucketedInputIterator<K, V> implements Iterator<KV<byte[], Iterator<V>>> {
  private final SortedBucketSource.BucketedInput<K, V> source;

  public final TupleTag<?> tupleTag;
  public final boolean emitByDefault;

  private final KeyGroupIterator<byte[], V> iter;
  final SortedBucketSource.Predicate<V> predicate;
  private KV<byte[], Iterator<V>> head = null;

  public KellenBucketedInputIterator(
      SortedBucketSource.BucketedInput<K, V>  source,
      int bucketId,
      int parallelism
  ) {
    this.source = source;
    this.predicate = this.source.predicate;
    this.tupleTag = source.getTupleTag();
    this.iter = this.source.createIterator(bucketId, parallelism);

    // advance iterator so we can peek at the key
    if(iter.hasNext()) head = iter.next();

    int numBuckets = source.getOrComputeMetadata().getCanonicalMetadata().getNumBuckets();
    // The canonical # buckets for this source. If # buckets >= the parallelism of the job,
    // we know that the key doesn't need to be re-hashed as it's already in the right bucket.
    this.emitByDefault = numBuckets >= parallelism;
  }

  public boolean hasPredicate() {
    return this.predicate != null;
  }

  public byte[] currentKey() {
    return head.getKey();
  }

  public boolean isExhausted() {
    return head == null;
  }

  @Override
  public boolean hasNext() {
    return head != null;
  }

  @Override
  public KV<byte[], Iterator<V>> next() {
    KV<byte[], Iterator<V>> curr = head;
    if(iter.hasNext()) {
      head = iter.next();
    } else {
      head = null;
    }
    return curr;
  }
}
