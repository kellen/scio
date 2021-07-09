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
          outputTransform(mergedKeyGroup, context, outputCollector, window);

          // exhaust iterators if necessary before moving on to the next key group:
          // for example, if not every element was needed in the transformFn
          sources.forEach(source -> {
            final Iterable<?> maybeUnfinishedIt = mergedKeyGroup.getValue().getAll(source.getTupleTag());
            if (SortedBucketSource.TraversableOnceIterable.class.isAssignableFrom(
                maybeUnfinishedIt.getClass())) {
              ((SortedBucketSource.TraversableOnceIterable<?>) maybeUnfinishedIt).ensureExhausted();
            }
          });
          out.output(new KellenBucketTransform.MergedBucket(bucketId, dst, effectiveParallelism));
        } catch (Exception ex) {
          throw new RuntimeException("Failed to write merged key group", ex);
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
