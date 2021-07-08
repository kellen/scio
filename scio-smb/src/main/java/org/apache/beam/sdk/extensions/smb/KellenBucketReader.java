package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;

public class KellenBucketReader extends BoundedSource.BoundedReader<KellenBucketItem> {
  private final BoundedSource<KellenBucketItem> currentSource;
  private boolean started;
  // there is only ever a single item in this source
  @Nullable private KellenBucketItem next;

  public KellenBucketReader(
      BoundedSource<KellenBucketItem> initialSource,
      int bucketOffsetId, int effectiveParallelism
  ) {
    this.currentSource = initialSource;
    this.started = false;
    this.next = new KellenBucketItem(bucketOffsetId, effectiveParallelism);
  }

  @Override
  public boolean start() throws IOException {
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    if(!started && next != null) {
      started = true;
      return true;
    } else {
      next = null;
      return false;
    }
  }

  @Override
  public KellenBucketItem getCurrent() throws NoSuchElementException {
    if (!started || next == null) {
      throw new NoSuchElementException();
    }
    return next;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public BoundedSource<KellenBucketItem> getCurrentSource() {
    return currentSource;
  }
}
