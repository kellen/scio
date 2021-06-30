package org.apache.beam.sdk.extensions.smb;


import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;


import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KellenBucketTransform<FinalKeyT, FinalValueT> extends PTransform<PBegin, WriteResult> {
  private final KellenBucketSource bucketSource;

  public KellenBucketTransform() {
    bucketSource = new KellenBucketSource(

    );
  }

  @Override
  public WriteResult expand(final PBegin begin) {
    return WriteResult.fromTuple(
        begin.getPipeline()
          .apply("ExpandBuckets", Read.from(bucketSource))
    );
  }

}
