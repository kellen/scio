package org.apache.beam.sdk.extensions.smb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.RenameBuckets;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;


public class KellenBucketTransform<FinalKeyT, FinalValueT> extends PTransform<PBegin, WriteResult> {
  private final KellenBucketSource<FinalKeyT> bucketSource;
  private final DoFn<Iterable<MergedBucket>, KV<BucketShardId, ResourceId>> finalizeBuckets;
  private final ParDo.SingleOutput<KellenBucketItem, MergedBucket> doFn;

  public KellenBucketTransform(
      Class<FinalKeyT> finalKeyClass,
      List<SortedBucketSource.BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism,
      // TODO some better way to do this e.g. a builder
      TransformFn<FinalKeyT, FinalValueT> transformFn,
      TransformFnWithSideInputContext<FinalKeyT, FinalValueT> sideInputTransformFn,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      Iterable<PCollectionView<?>> sides,
      NewBucketMetadataFn<FinalKeyT, FinalValueT> newBucketMetadataFn,
      FileOperations<FinalValueT> fileOperations,
      String filenameSuffix,
      String filenamePrefix) {
    // TODO make this impossible by construction
    assert !((transformFn == null) && (sideInputTransformFn == null)); // at least one defined
    assert !((transformFn != null) && (sideInputTransformFn != null)); // only one defined

    final SMBFilenamePolicy filenamePolicy = new SMBFilenamePolicy(outputDirectory, filenamePrefix, filenameSuffix);
    final SourceSpec<FinalKeyT> sourceSpec = SourceSpec.from(finalKeyClass, sources);
    bucketSource = new KellenBucketSource<>(sources, targetParallelism, 1, 0, sourceSpec, -1);
    finalizeBuckets = new FinalizeTransformedBuckets<>(fileOperations, newBucketMetadataFn, filenamePolicy.forDestination(), sourceSpec.hashType);

    final FileAssignment fileAssignment = filenamePolicy.forTempFiles(tempDirectory);
    final Distribution dist = Metrics.distribution(getName(), getName() + "-KeyGroupSize");
    if(transformFn != null) {
      this.doFn = ParDo.of(new KellenTransformDoFn.NoSides<>(sources, sourceSpec, fileAssignment, fileOperations, transformFn, dist));
    } else {
      this.doFn =
          ParDo.of(new KellenTransformDoFn.WithSides<>(sources, sourceSpec, fileAssignment, fileOperations, sideInputTransformFn, dist))
              .withSideInputs(sides);
    }
  }

  @Override
  public WriteResult expand(final PBegin begin) {
    return WriteResult.fromTuple(
        begin.getPipeline()
            // outputs bucket offsets for the various SMB readers
            .apply("BucketOffsets", Read.from(bucketSource))
            .apply("Foo", this.doFn)
            .apply(Filter.by(Objects::nonNull))
            .apply(Group.globally())
            .apply(
                "FinalizeTempFiles",
                ParDo.of(finalizeBuckets)
                    .withOutputTags(
                        FinalizeTransformedBuckets.BUCKETS_TAG,
                        TupleTagList.of(FinalizeTransformedBuckets.METADATA_TAG))));

  }

  @FunctionalInterface
  public interface TransformFn<KeyT, ValueT> extends Serializable {
    void writeTransform(
        KV<KeyT, CoGbkResult> keyGroup,
        SerializableConsumer<ValueT> outputConsumer
    );
  }

  @FunctionalInterface
  public interface TransformFnWithSideInputContext<KeyT, ValueT> extends Serializable {
    void writeTransform(
        KV<KeyT, CoGbkResult> keyGroup,
        DoFn<KellenBucketItem, MergedBucket>.ProcessContext c,
        SerializableConsumer<ValueT> outputConsumer,
        BoundedWindow window
    );
  }

  // pure copy-paste

  public interface NewBucketMetadataFn<K, V> extends Serializable {
    public BucketMetadata<K, V> createMetadata(int numBuckets, int numShards, HashType hashType)
        throws CannotProvideCoderException, NonDeterministicException;
  }

  private static class FinalizeTransformedBuckets<FinalValueT>
    extends DoFn<Iterable<MergedBucket>, KV<BucketShardId, ResourceId>> {
  private final FileOperations<FinalValueT> fileOperations;
  private final NewBucketMetadataFn<?, ?> newBucketMetadataFn;
  private final FileAssignment dstFileAssignment;
  private final HashType hashType;

  static final TupleTag<KV<BucketShardId, ResourceId>> BUCKETS_TAG =
      new TupleTag<>("writtenBuckets");
  static final TupleTag<ResourceId> METADATA_TAG = new TupleTag<>("writtenMetadata");

  public FinalizeTransformedBuckets(
      FileOperations<FinalValueT> fileOperations,
      NewBucketMetadataFn<?, ?> newBucketMetadataFn,
      FileAssignment dstFileAssignment,
      HashType hashType) {
    this.fileOperations = fileOperations;
    this.newBucketMetadataFn = newBucketMetadataFn;
    this.dstFileAssignment = dstFileAssignment;
    this.hashType = hashType;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    final Iterator<MergedBucket> mergedBuckets = c.element().iterator();
    final Map<BucketShardId, ResourceId> writtenBuckets = new HashMap<>();

    BucketMetadata<?, ?> bucketMetadata = null;
    while (mergedBuckets.hasNext()) {
      final MergedBucket bucket = mergedBuckets.next();
      if (bucketMetadata == null) {
        try {
          bucketMetadata =
              newBucketMetadataFn.createMetadata(bucket.totalNumBuckets, 1, hashType);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      writtenBuckets.put(BucketShardId.of(bucket.bucketId, 0), bucket.destination);
    }

    RenameBuckets.moveFiles(
        bucketMetadata,
        writtenBuckets,
        dstFileAssignment,
        fileOperations,
        bucketDst -> c.output(BUCKETS_TAG, bucketDst),
        metadataDst -> c.output(METADATA_TAG, metadataDst),
        false); // Don't include null-key bucket in output
  }
}

// TODO made public
  public static class MergedBucket implements Serializable {
    final ResourceId destination;
    final int bucketId;
    final int totalNumBuckets;

    MergedBucket(Integer bucketId, ResourceId destination, Integer totalNumBuckets) {
      this.destination = destination;
      this.bucketId = bucketId;
      this.totalNumBuckets = totalNumBuckets;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MergedBucket that = (MergedBucket) o;
      return Objects.equals(destination, that.destination)
          && Objects.equals(bucketId, that.bucketId)
          && Objects.equals(totalNumBuckets, that.totalNumBuckets);
    }

    @Override
    public int hashCode() {
      return Objects.hash(destination, bucketId, totalNumBuckets);
    }
  }

  public interface SerializableConsumer<ValueT> extends Consumer<ValueT>, Serializable {}
}
