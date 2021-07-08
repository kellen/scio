package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;


public class KellenTransformDoFn {
//  public static class NoSides<FinalKeyT, FinalValueT> extends DoFn<KellenBucketItem, KellenBucketTransform.MergedBucket> {
//    private final SMBFilenamePolicy.FileAssignment fileAssignment;
//    private final FileOperations<FinalValueT> fileOperations;
//    private final KellenBucketTransform.TransformFn<FinalKeyT, FinalValueT> transformFn;
//    private final Coder<FinalKeyT> keyCoder;
//    private final List<SortedBucketSource.BucketedInput<?, ?>> sources;
//    private final Distribution keyGroupSize;
//    boolean materializeKeyGroup;
//    private Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();
//
//    public NoSides(
//        List<SortedBucketSource.BucketedInput<?, ?>> sources,
//        SourceSpec<FinalKeyT> sourceSpec,
//        SMBFilenamePolicy.FileAssignment fileAssignment,
//        FileOperations<FinalValueT> fileOperations,
//        KellenBucketTransform.TransformFn<FinalKeyT, FinalValueT> transformFn,
//        Distribution keyGroupSize,
//        boolean materializeKeyGroup
//    ) {
//      this.sources = sources;
//      this.fileAssignment = fileAssignment;
//      this.fileOperations = fileOperations;
//      this.transformFn = transformFn;
//      this.keyCoder = sourceSpec.keyCoder;
//      this.keyGroupSize = keyGroupSize;
//      this.materializeKeyGroup = materializeKeyGroup;
//    }
//
//    private enum AcceptKeyGroup { ACCEPT, REJECT, UNSET }
//
//    @ProcessElement
//    public void processElement(@Element KellenBucketItem e, OutputReceiver<KellenBucketTransform.MergedBucket> out, ProcessContext c) {
//      // here do all the reader shit that was done previously & hand off context for
//      // parts from BoundedReader<MergedBucket> and the application of the transformfunction
//      int bucketId = e.bucketOffsetId;
//      int effectiveParallelism = e.effectiveParallelism;
//      // TODO remove and replace with effectiveParallelism
//      int parallelism = effectiveParallelism;
//
//      // from SortedBucketTransform.MerageAndWriteBucketSource.createReader().new BoundedReader().start()
//      try {
//        // source TupleTags `and`-ed together
//        CoGbkResultSchema resultSchema = SortedBucketSource.BucketedInput.schemaOf(sources);
//        // source TupleTags in the order in which sources are specified
//        TupleTagList tupleTags = resultSchema.getTupleTagList();
//
//        ResourceId dst = fileAssignment.forBucket(BucketShardId.of(bucketId, 0), effectiveParallelism, 1);
//        OutputCollector<FinalValueT> outputCollector = new OutputCollector<>(fileOperations.createWriter(dst));
//
//        List<KellenBucketedInputIterator<?, ?>> foobar =
//            sources.stream()
//                .map(src -> new KellenBucketedInputIterator<>(src, bucketId, effectiveParallelism))
//                .collect(Collectors.toList());
//
//        // TODO document
//        Function<byte[], Boolean> keyGroupFilter =
//            (bytes) -> sources.get(0).getMetadata().rehashBucket(bytes, effectiveParallelism) == bucketId;
//
//        while(true) {
//          // complete once all sources are exhausted
//          boolean allExhausted = foobar.stream().allMatch(src -> src.isExhausted());
//          if(allExhausted) break;
//
//          // we process keys in order, but since not all sources have all keys, find the minimum available key
//          byte[] minKey = foobar.stream().map(src -> src.currentKey()).min(bytesComparator).orElse(null);
//          final Boolean emitBasedOnMinKeyBucketing = keyGroupFilter.apply(minKey);
//
//          // output accumulator
//          final List<Iterable<?>> valueMap =
//              IntStream.range(0, resultSchema.size()).mapToObj(i -> new ArrayList<>()).collect(Collectors.toList());
//
//          AcceptKeyGroup acceptKeyGroup = AcceptKeyGroup.UNSET;
//          for(KellenBucketedInputIterator<?, ?> src : foobar) {
//            // for each source, if the current key is equal to the minimum, consume it
//            if(bytesComparator.compare(minKey, src.currentKey()) == 0) {
//              // TODO this check seems a little weird, not sure if skipping it by keeping `acceptKeyGroup` around is helpful
//              // if this key group has been previously accepted by a preceding source, emit.
//              // "  "    "   "     "   "    "          rejected by a preceding source, don't emit.
//              // if this is the first source for this key group, emit if either the source settings or the min key say we should.
//              boolean emitKeyGroup = (acceptKeyGroup == AcceptKeyGroup.ACCEPT) ||
//                                     ((acceptKeyGroup == AcceptKeyGroup.UNSET) && (src.emitByDefault || emitBasedOnMinKeyBucketing));
//
//              final Iterator<Object> keyGroupIterator = (Iterator<Object>) src.next().getValue();
//              if(emitKeyGroup) {
//                acceptKeyGroup = AcceptKeyGroup.ACCEPT;
//                // data must be materialized (eagerly evaluated) if requested or if there is a predicate
//                boolean materialize = materializeKeyGroup || src.hasPredicate();
//                int outputIndex = resultSchema.getIndex(src.tupleTag);
//
//                if(!materialize) {
//                  // lazy data iterator
//                  valueMap.set(
//                      outputIndex,
//                      new SortedBucketSource.TraversableOnceIterable<>(
//                          Iterators.transform(
//                              keyGroupIterator, (value) -> {
//                                // TODO runningKeyGroupSize++;
//                                return value;
//                              }
//                          )
//                      )
//                  );
//
//                } else {
//                  // eagerly materialize this iterator into `List<Object>` by applying the predicate to each value
//                  final List<Object> values = (List<Object>) valueMap.get(outputIndex);
//                  keyGroupIterator.forEachRemaining(v -> {
//                    if (((SortedBucketSource.Predicate<Object>) src.predicate).apply(values, v)) {
//                          values.add(v);
//                          // TODO runningKeyGroupSize++;
//                        }
//                  });
//                }
//              } else {
//                acceptKeyGroup = AcceptKeyGroup.REJECT;
//                // skip key but still have to exhaust iterator
//                keyGroupIterator.forEachRemaining(value -> {});
//              }
//            }
//          }
//
//          if(acceptKeyGroup == AcceptKeyGroup.ACCEPT) {
//            final KV<byte[], CoGbkResult> xxx = KV.of(minKey, CoGbkResultUtil.newCoGbkResult(resultSchema, valueMap));
//          }
//        }
//      } catch (IOException err) {
//        throw new RuntimeException(err);
//      }
//
//      // from SortedBucketSource
//      Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();
//      // from SortedBucketSource.MergeBucketsReader
//      Comparator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> keyComparator =
//          (o1, o2) -> bytesComparator.compare(o1.getValue().getKey(), o2.getValue().getKey());
//      // from SortedBucketSource.MergeBucketsReader.MergeBucketsReader()
//      int numSources = sources.size();
//      Function<byte[], Boolean> keyGroupFilter = (bytes) -> sources.get(0)
//                                                                .getMetadata()
//                                                                .rehashBucket(bytes, effectiveParallelism) == bucketId;
//      SortedBucketSource.Predicate[] predicates = sources.stream()
//          .map(i -> i.predicate)
//          .toArray(SortedBucketSource.Predicate[]::new);
//      KeyGroupIterator[] iterators = sources.stream()
//          .map(i -> i.createIterator(bucketId, effectiveParallelism))
//          .toArray(KeyGroupIterator[]::new);
//      CoGbkResultSchema resultSchema = SortedBucketSource.BucketedInput.schemaOf(sources);
//      TupleTagList tupleTags = resultSchema.getTupleTagList();
//      Map<TupleTag, Integer> bucketsPerSource =
//          sources.stream()
//              .collect(
//                  Collectors.toMap(
//                      SortedBucketSource.BucketedInput::getTupleTag,
//                      i -> i.getOrComputeMetadata().getCanonicalMetadata().getNumBuckets()));
//
//      // from SortedBucketSource.MergeBucketsReader.start()
//      Map<TupleTag, KV<byte[], Iterator<?>>> nextKeyGroups = new HashMap<>();
//
//      // from SortedBucketSource.MergeBucketsReader.MergeBucketsReader()
//      int runningKeyGroupSize = 0;
//
//      // from SortedBucketSource.MergeBucketsReader.advance()
//      while (true) {
//        if (runningKeyGroupSize != 0) { // If it's 0, that means we haven't started reading
//          keyGroupSize.update(runningKeyGroupSize);
//          runningKeyGroupSize = 0;
//        }
//
//        int completedSources = 0;
//        // Advance key-value groups from each source
//        for (int i = 0; i < numSources; i++) {
//          final KeyGroupIterator it = iterators[i];
//          if (nextKeyGroups.containsKey(tupleTags.get(i))) {
//            continue;
//          }
//          if (it.hasNext()) {
//            final KV<byte[], Iterator<?>> next = it.next();
//            nextKeyGroups.put(tupleTags.get(i), next);
//          } else {
//            completedSources++;
//          }
//        }
//
//        if (nextKeyGroups.isEmpty()) {
//          break;
//        }
//
//        // Find next key-value groups
//        final Map.Entry<TupleTag, KV<byte[], Iterator<?>>> minKeyEntry =
//            nextKeyGroups.entrySet().stream().min(keyComparator).orElse(null);
//
//        final Iterator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> nextKeyGroupsIt =
//            nextKeyGroups.entrySet().iterator();
//        final List<Iterable<?>> valueMap = new ArrayList<>();
//        for (int i = 0; i < resultSchema.size(); i++) {
//          valueMap.add(new ArrayList<>());
//        }
//
//        // Set to 1 if subsequent key groups should be accepted or 0 if they should be filtered out
//        int acceptKeyGroup = -1;
//
//        while (nextKeyGroupsIt.hasNext()) {
//          final Map.Entry<TupleTag, KV<byte[], Iterator<?>>> entry = nextKeyGroupsIt.next();
//
//          if (keyComparator.compare(entry, minKeyEntry) == 0) {
//            final TupleTag tupleTag = entry.getKey();
//            int index = resultSchema.getIndex(tupleTag);
//
//            // Track the canonical # buckets of each source that the key is found in.
//            // If we find it in a source with a # buckets >= the parallelism of the job,
//            // we know that it doesn't need to be re-hashed as it's already in the right bucket.
//            final boolean emitKeyGroup =
//                acceptKeyGroup == 1
//                || (acceptKeyGroup != 0 // make sure it hasn't already been ruled out
//                    && (bucketsPerSource.get(tupleTag) >= parallelism
//                        || keyGroupFilter.apply(minKeyEntry.getValue().getKey())));
//
//            // If the user supplies a predicate, we have to materialize the iterable to apply it
//            boolean materialize = (materializeKeyGroup || predicates[index] != null);
//
//            final SortedBucketSource.Predicate<Object> predicate =
//                predicates[index] != null ? predicates[index] : (xs, x) -> true;
//
//            final Iterator<Object> keyGroupIterator = (Iterator<Object>) entry.getValue().getValue();
//
//            if (emitKeyGroup && !materialize) {
//              valueMap.set(
//                  index,
//                  new SortedBucketSource.TraversableOnceIterable<>(
//                      Iterators.transform(
//                          keyGroupIterator,
//                          (value) -> {
//                            runningKeyGroupSize++;
//                            return value;
//                          })));
//              acceptKeyGroup = 1;
//            } else if (emitKeyGroup) {
//              final List<Object> values = (List<Object>) valueMap.get(index);
//              keyGroupIterator.forEachRemaining(
//                  v -> {
//                    if (predicate.apply(values, v)) {
//                      values.add(v);
//                      runningKeyGroupSize++;
//                    }
//                  });
//              acceptKeyGroup = 1;
//            } else {
//              // skip key but still have to exhaust iterator
//              keyGroupIterator.forEachRemaining(value -> {});
//              acceptKeyGroup = 0;
//            }
//
//            nextKeyGroupsIt.remove();
//          }
//        }
//
//        if (acceptKeyGroup == 1) {
//          KV<byte[], CoGbkResult> next =
//              KV.of(
//                  minKeyEntry.getValue().getKey(),
//                  CoGbkResultUtil.newCoGbkResult(resultSchema, valueMap));
//
//          // from SortedBucketTransform.MerageAndWriteBucketSource.createReader().new BoundedReader().getCurrent()
//          try {
//            //KV<FinalKeyT, CoGbkResult> mergedKeyGroup = keyGroupReader.getCurrent();
//            // expands to:
//            try {
//              KV<FinalKeyT, CoGbkResult> mergedKeyGroup = KV.of(
//                  keyCoder.decode(new ByteArrayInputStream(next.getKey())), next.getValue()
//              );
//              transformFn.writeTransform(mergedKeyGroup, outputCollector);
//
//              // exhaust iterators if necessary before moving on to the next key group:
//              // for example, if not every element was needed in the transformFn
//              sources.forEach(
//                  source -> {
//                    final Iterable<?> maybeUnfinishedIt =
//                        mergedKeyGroup.getValue().getAll(source.getTupleTag());
//                    if (SortedBucketSource.TraversableOnceIterable.class.isAssignableFrom(
//                        maybeUnfinishedIt.getClass())) {
//                      ((SortedBucketSource.TraversableOnceIterable<?>) maybeUnfinishedIt).ensureExhausted();
//                    }
//                  });
//
//                  out.output(new KellenBucketTransform.MergedBucket(bucketId, dst, effectiveParallelism));
//              } catch (Exception exxx) {
//                throw new RuntimeException("Failed to write merged key group", exxx);
//              }
//            } catch (Exception err) {
//              throw new RuntimeException("Failed to decode key group", err);
//            }
//        } else {
//          if (completedSources == numSources) {
//            break;
//          }
//        }
//      }
//
//    }
//  }

  public static abstract class FML<FinalKeyT, FinalValueT> extends DoFn<KellenBucketItem, KellenBucketTransform.MergedBucket> {
    protected final SMBFilenamePolicy.FileAssignment fileAssignment;
    protected final FileOperations<FinalValueT> fileOperations;
    protected final List<SortedBucketSource.BucketedInput<?, ?>> sources;
    protected final Distribution keyGroupSize;
    protected final SourceSpec<FinalKeyT> sourceSpec;

    protected FML(
        List<SortedBucketSource.BucketedInput<?, ?>> sources,
        SourceSpec<FinalKeyT> sourceSpec,
        SMBFilenamePolicy.FileAssignment fileAssignment,
        FileOperations<FinalValueT> fileOperations,
        Distribution keyGroupSize
    ) {
      this.fileAssignment = fileAssignment;
      this.fileOperations = fileOperations;
      this.sources = sources;
      this.keyGroupSize = keyGroupSize;
      this.sourceSpec = sourceSpec;
    }

    protected abstract void outputTransform(
        KV<FinalKeyT, CoGbkResult> mergedKeyGroup,
        ProcessContext context,
        OutputCollector<FinalValueT> outputCollector,
        BoundedWindow window
    );

    @ProcessElement
    public void processElement(
        @Element KellenBucketItem e,
        OutputReceiver<KellenBucketTransform.MergedBucket> out,
        ProcessContext context,
        BoundedWindow window
    ) {
      int bucketId = e.bucketOffsetId;
      int effectiveParallelism = e.effectiveParallelism;

      ResourceId dst = fileAssignment.forBucket(BucketShardId.of(bucketId, 0), effectiveParallelism, 1);
      OutputCollector<FinalValueT> outputCollector;
      try {
        outputCollector = new OutputCollector<>(fileOperations.createWriter(dst));
      } catch (IOException err) {
        throw new RuntimeException(err);
      }

      final KellenMultiSourceKeyGroupIterator<FinalKeyT> iter = new KellenMultiSourceKeyGroupIterator<>(sources, sourceSpec, keyGroupSize, false, bucketId, effectiveParallelism);
      while(iter.hasNext()) {
        try {
          KV<FinalKeyT, CoGbkResult> mergedKeyGroup = iter.next();
          outputTransform(mergedKeyGroup, context, outputCollector);

          // exhaust iterators if necessary before moving on to the next key group:
          // for example, if not every element was needed in the transformFn
          sources.forEach(
              source -> {
                final Iterable<?> maybeUnfinishedIt = mergedKeyGroup.getValue().getAll(source.getTupleTag());
                if (SortedBucketSource.TraversableOnceIterable.class.isAssignableFrom(
                    maybeUnfinishedIt.getClass())) {
                  ((SortedBucketSource.TraversableOnceIterable<?>) maybeUnfinishedIt).ensureExhausted();
                }
              });

          out.output(new KellenBucketTransform.MergedBucket(bucketId, dst, effectiveParallelism));
        } catch (Exception exxx) {
          throw new RuntimeException("Failed to write merged key group", exxx);
        }
      }
    }
  }

  public static class NoSides<FinalKeyT, FinalValueT> extends FML<FinalKeyT, FinalValueT> {
    private final KellenBucketTransform.TransformFn<FinalKeyT, FinalValueT> transformFn;
    public NoSides(
        List<SortedBucketSource.BucketedInput<?, ?>> sources,
        SourceSpec<FinalKeyT> sourceSpec,
        SMBFilenamePolicy.FileAssignment fileAssignment,
        FileOperations<FinalValueT> fileOperations,
        KellenBucketTransform.TransformFn<FinalKeyT, FinalValueT> transformFn,
        Distribution keyGroupSize
    ) {
      super(sources, sourceSpec, fileAssignment, fileOperations, keyGroupSize);
      this.transformFn = transformFn;
    }

    @Override
    protected void outputTransform(
        final KV<FinalKeyT, CoGbkResult> mergedKeyGroup,
        final ProcessContext context,
        final OutputCollector<FinalValueT> outputCollector,
        final BoundedWindow window
    ) {
      transformFn.writeTransform(mergedKeyGroup, outputCollector);
    }
  }

  public static class WithSides<FinalKeyT, FinalValueT> extends FML<FinalKeyT, FinalValueT> {
    private final KellenBucketTransform.TransformFnWithSideInputContext<FinalKeyT, FinalValueT> transformFn;
    public WithSides(
        List<SortedBucketSource.BucketedInput<?, ?>> sources,
        SourceSpec<FinalKeyT> sourceSpec,
        SMBFilenamePolicy.FileAssignment fileAssignment,
        FileOperations<FinalValueT> fileOperations,
        KellenBucketTransform.TransformFnWithSideInputContext<FinalKeyT, FinalValueT> transformFn,
        Distribution keyGroupSize
    ) {
      super(sources, sourceSpec, fileAssignment, fileOperations, keyGroupSize);
      this.transformFn = transformFn;
    }

    @Override
    protected void outputTransform(
        final KV<FinalKeyT, CoGbkResult> mergedKeyGroup,
        final ProcessContext context,
        final OutputCollector<FinalValueT> outputCollector,
        final BoundedWindow window
    ) {
      transformFn.writeTransform(mergedKeyGroup, context, outputCollector, window);
    }
  }

//  public static class WithSides<FinalKeyT, FinalValueT> extends DoFn<KellenBucketItem, KellenBucketTransform.MergedBucket> {
//    private final SMBFilenamePolicy.FileAssignment fileAssignment;
//    private final FileOperations<FinalValueT> fileOperations;
//    private final KellenBucketTransform.TransformFnWithSideInputContext<FinalKeyT, FinalValueT> transformFn;
//    private final List<SortedBucketSource.BucketedInput<?, ?>> sources;
//    private final Distribution keyGroupSize;
//    final private SourceSpec<FinalKeyT> sourceSpec;
//
//    public WithSides(
//        List<SortedBucketSource.BucketedInput<?, ?>> sources,
//        SourceSpec<FinalKeyT> sourceSpec,
//        SMBFilenamePolicy.FileAssignment fileAssignment,
//        FileOperations<FinalValueT> fileOperations,
//        KellenBucketTransform.TransformFnWithSideInputContext<FinalKeyT, FinalValueT> transformFn,
//        Distribution keyGroupSize
//    ) {
//      this.sources = sources;
//      this.sourceSpec = sourceSpec;
//      this.fileAssignment = fileAssignment;
//      this.fileOperations = fileOperations;
//      this.transformFn = transformFn;
//      this.keyGroupSize = keyGroupSize;
//    }
//
//    @ProcessElement
//    public void processElement(@Element KellenBucketItem e, OutputReceiver<KellenBucketTransform.MergedBucket> out, ProcessContext context) {
//      int bucketId = e.bucketOffsetId;
//      int effectiveParallelism = e.effectiveParallelism;
//
//      ResourceId dst = fileAssignment.forBucket(BucketShardId.of(bucketId, 0), effectiveParallelism, 1);
//      OutputCollector<FinalValueT> outputCollector;
//      try {
//        outputCollector = new OutputCollector<>(fileOperations.createWriter(dst));
//      } catch (IOException err) {
//        throw new RuntimeException(err);
//      }
//
//      final KellenMultiSourceKeyGroupIterator<FinalKeyT> iter = new KellenMultiSourceKeyGroupIterator<>(sources, sourceSpec, keyGroupSize, false, bucketId, effectiveParallelism);
//      while(iter.hasNext()) {
//        try {
//          KV<FinalKeyT, CoGbkResult> mergedKeyGroup = iter.next();
//          transformFn.writeTransform(mergedKeyGroup, context, outputCollector);
//
//          // exhaust iterators if necessary before moving on to the next key group:
//          // for example, if not every element was needed in the transformFn
//          sources.forEach(
//              source -> {
//                final Iterable<?> maybeUnfinishedIt = mergedKeyGroup.getValue().getAll(source.getTupleTag());
//                if (SortedBucketSource.TraversableOnceIterable.class.isAssignableFrom(
//                    maybeUnfinishedIt.getClass())) {
//                  ((SortedBucketSource.TraversableOnceIterable<?>) maybeUnfinishedIt).ensureExhausted();
//                }
//              });
//
//          out.output(new KellenBucketTransform.MergedBucket(bucketId, dst, effectiveParallelism));
//        } catch (Exception exxx) {
//          throw new RuntimeException("Failed to write merged key group", exxx);
//        }
//      }
//    }
//  }

  // copy-paste from SortedBucketTransform.java
  private static class OutputCollector<ValueT> implements KellenBucketTransform.SerializableConsumer<ValueT> {
    private final FileOperations.Writer<ValueT> writer;

    OutputCollector(FileOperations.Writer<ValueT> writer) {
      this.writer = writer;
    }

    void onComplete() {
      try {
        writer.close();
      } catch (IOException e) {
        throw new RuntimeException("Closing writer failed: ", e);
      }
    }

    @Override
    public void accept(ValueT t) {
      try {
        writer.write(t);
      } catch (IOException e) {
        throw new RuntimeException("Write of element " + t + " failed: ", e);
      }
    }
  }

}
