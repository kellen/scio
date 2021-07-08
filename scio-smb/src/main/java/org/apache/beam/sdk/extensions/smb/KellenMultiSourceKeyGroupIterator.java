package org.apache.beam.sdk.extensions.smb;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;

public class KellenMultiSourceKeyGroupIterator<FinalKeyT> implements Iterator<KV<FinalKeyT, CoGbkResult>> {
  private enum AcceptKeyGroup { ACCEPT, REJECT, UNSET }

  private Optional<KV<FinalKeyT, CoGbkResult>> head = null;

//  private final SMBFilenamePolicy.FileAssignment fileAssignment;
//  private final FileOperations<FinalValueT> fileOperations;
  private final Coder<FinalKeyT> keyCoder;
//  private final List<SortedBucketSource.BucketedInput<?, ?>> sources;
  // TODO
  private final Distribution keyGroupSize;
  boolean materializeKeyGroup;
  private Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();

  private final CoGbkResultSchema resultSchema;
  private final List<KellenBucketedInputIterator<?, ?>> foobar;
  private final Function<byte[], Boolean> keyGroupFilter;

  public KellenMultiSourceKeyGroupIterator(
      List<SortedBucketSource.BucketedInput<?, ?>> sources,
       SourceSpec<FinalKeyT> sourceSpec,
//       SMBFilenamePolicy.FileAssignment fileAssignment,
//       FileOperations<FinalValueT> fileOperations,
       Distribution keyGroupSize,
       boolean materializeKeyGroup,
      int bucketId,
      int effectiveParallelism
  ) {
//    this.sources = sources;
//    this.fileAssignment = fileAssignment;
//    this.fileOperations = fileOperations;
    this.keyCoder = sourceSpec.keyCoder;
    this.keyGroupSize = keyGroupSize;
    this.materializeKeyGroup = materializeKeyGroup;

    // source TupleTags `and`-ed together
    this.resultSchema = SortedBucketSource.BucketedInput.schemaOf(sources);
    // TODO rename
    this.foobar =
        sources.stream()
            .map(src -> new KellenBucketedInputIterator<>(src, bucketId, effectiveParallelism))
            .collect(Collectors.toList());
    // TODO document
    this.keyGroupFilter = (bytes) -> sources.get(0).getMetadata().rehashBucket(bytes, effectiveParallelism) == bucketId;

    advance();
  }

  @Override
  public boolean hasNext() {
    return head.isPresent();
  }

  @Override
  public KV<FinalKeyT, CoGbkResult> next() {
    if(head == null) throw new IllegalStateException("Iterator head not yet initialized.");
    if(!head.isPresent()) throw new NoSuchElementException();

    KV<FinalKeyT, CoGbkResult> cur = head.get();
    advance();
    return cur;
  }

  private void advance() {
    while(true) {
      // complete once all sources are exhausted
      boolean allExhausted = foobar.stream().allMatch(src -> src.isExhausted());
      if(allExhausted) {
        head = Optional.empty();
        break;
      }

      // we process keys in order, but since not all sources have all keys, find the minimum available key
      byte[] minKey = foobar.stream().map(src -> src.currentKey()).min(bytesComparator).orElse(null);
      final Boolean emitBasedOnMinKeyBucketing = keyGroupFilter.apply(minKey);

      // output accumulator
      final List<Iterable<?>> valueMap =
          IntStream.range(0, resultSchema.size()).mapToObj(i -> new ArrayList<>()).collect(Collectors.toList());

      AcceptKeyGroup acceptKeyGroup = AcceptKeyGroup.UNSET;
      for(KellenBucketedInputIterator<?, ?> src : foobar) {
        // for each source, if the current key is equal to the minimum, consume it
        if(bytesComparator.compare(minKey, src.currentKey()) == 0) {
          // TODO this check seems a little weird, not sure if skipping it by keeping `acceptKeyGroup` around is helpful
          // if this key group has been previously accepted by a preceding source, emit.
          // "  "    "   "     "   "    "          rejected by a preceding source, don't emit.
          // if this is the first source for this key group, emit if either the source settings or the min key say we should.
          boolean emitKeyGroup = (acceptKeyGroup == AcceptKeyGroup.ACCEPT) ||
                                 ((acceptKeyGroup == AcceptKeyGroup.UNSET) && (src.emitByDefault || emitBasedOnMinKeyBucketing));

          final Iterator<Object> keyGroupIterator = (Iterator<Object>) src.next().getValue();
          if(emitKeyGroup) {
            acceptKeyGroup = AcceptKeyGroup.ACCEPT;
            // data must be materialized (eagerly evaluated) if requested or if there is a predicate
            boolean materialize = materializeKeyGroup || src.hasPredicate();
            int outputIndex = resultSchema.getIndex(src.tupleTag);

            if(!materialize) {
              // lazy data iterator
              valueMap.set(
                  outputIndex,
                  new SortedBucketSource.TraversableOnceIterable<>(
                      Iterators.transform(
                          keyGroupIterator, (value) -> {
                            // TODO runningKeyGroupSize++;
                            return value;
                          }
                      )
                  )
              );

            } else {
              // TODO unclear why this must be eager
              // eagerly materialize this iterator into `List<Object>` by applying the predicate to each value
              final List<Object> values = (List<Object>) valueMap.get(outputIndex);
              keyGroupIterator.forEachRemaining(v -> {
                if (((SortedBucketSource.Predicate<Object>) src.predicate).apply(values, v)) {
                  values.add(v);
                  // TODO runningKeyGroupSize++;
                }
              });
            }
          } else {
            acceptKeyGroup = AcceptKeyGroup.REJECT;
            // skip key but still have to exhaust iterator
            keyGroupIterator.forEachRemaining(value -> {});
          }
        }
      }

      if(acceptKeyGroup == AcceptKeyGroup.ACCEPT) {
        final KV<byte[], CoGbkResult> next = KV.of(minKey, CoGbkResultUtil.newCoGbkResult(resultSchema, valueMap));
        try {
          // new head found, we're done
          head = Optional.of(KV.of(keyCoder.decode(new ByteArrayInputStream(next.getKey())), next.getValue()));
          break;
        } catch (Exception e) {
          throw new RuntimeException("Failed to decode key group", e);
        }
      }
    }
  }
}
