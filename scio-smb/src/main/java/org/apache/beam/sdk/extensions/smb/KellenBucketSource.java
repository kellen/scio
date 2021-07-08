package org.apache.beam.sdk.extensions.smb;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source of integers up to numBuckets
 * but returning the estimated size in bytes
 */
public class KellenBucketSource<FinalKeyT> extends BoundedSource<KellenBucketItem> {
  // Dataflow calls split() with a suggested byte size that assumes a higher throughput than
  // SMB joins have. By adjusting this suggestion we can arrive at a more optimal parallelism.
  static final Double DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR = 0.33;

  private static final Logger LOG = LoggerFactory.getLogger(KellenBucketSource.class);

  private final List<SortedBucketSource.BucketedInput<?, ?>> sources;
  private final TargetParallelism targetParallelism;
  private final SourceSpec<FinalKeyT> sourceSpec;

  private final int effectiveParallelism;
  private final int bucketOffsetId;
  private long estimatedSizeBytes;

  public KellenBucketSource(
      List<SortedBucketSource.BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism,
      int effectiveParallelism,
      int bucketOffsetId,
      SourceSpec<FinalKeyT> sourceSpec,
      long estimatedSizeBytes
      ) {
    this.sources = sources;
    this.targetParallelism = targetParallelism;
    this.effectiveParallelism = effectiveParallelism;
    this.bucketOffsetId = bucketOffsetId;
    this.sourceSpec = sourceSpec;
    this.estimatedSizeBytes = estimatedSizeBytes;
  }

  public KellenBucketSource<FinalKeyT> split(int bucketOffsetId, int adjustedParallelism, long estimatedSizeBytes) {
    return new KellenBucketSource<>(sources, targetParallelism, adjustedParallelism, bucketOffsetId, sourceSpec, estimatedSizeBytes);
  }

  @Override
  public List<? extends BoundedSource<KellenBucketItem>> split(
      final long desiredBundleSizeBytes,
      final PipelineOptions options) throws Exception {

    final int numSplits = SortedBucketSource.getNumSplits(
            sourceSpec,
            effectiveParallelism,
            targetParallelism,
            getEstimatedSizeBytes(options),
            desiredBundleSizeBytes,
            DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR);

    final long estSplitSize = estimatedSizeBytes / numSplits;

    final DecimalFormat sizeFormat = new DecimalFormat("0.00");
    LOG.info(
        "Parallelism was adjusted by {}splitting source of size {} MB into {} source(s) of size {} MB",
        effectiveParallelism > 1 ? "further " : "",
        sizeFormat.format(estimatedSizeBytes / 1000000.0),
        numSplits,
        sizeFormat.format(estSplitSize / 1000000.0));

    final int totalParallelism = numSplits * effectiveParallelism;
    return IntStream.range(0, numSplits)
        .boxed()
        .map(
            i ->
                this.split(
                    bucketOffsetId + (i * effectiveParallelism), totalParallelism, estSplitSize))
        .collect(Collectors.toList());
  }

  @Override
  public Coder<KellenBucketItem> getOutputCoder() {
    return NullableCoder.of(SerializableCoder.of(KellenBucketItem.class));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("targetParallelism", targetParallelism.toString()));
  }

  @Override
  public long getEstimatedSizeBytes(final PipelineOptions options) throws Exception {
    if (estimatedSizeBytes == -1) {
      estimatedSizeBytes = sources.parallelStream().mapToLong(SortedBucketSource.BucketedInput::getOrSampleByteSize).sum();
      LOG.info("Estimated byte size is " + estimatedSizeBytes);
    }
    return estimatedSizeBytes;
  }

  @Override
  public BoundedReader<KellenBucketItem> createReader(final PipelineOptions options) throws IOException {
    return new KellenBucketReader(this, bucketOffsetId, effectiveParallelism);
  }
}