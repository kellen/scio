package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;

public class KellenBucketReader extends BoundedSource.BoundedReader<Integer> {
  private final BoundedSource<Integer> currentSource;
  private int index;
  @Nullable private Optional<Integer> next;

  public KellenBucketReader(
      BoundedSource<Integer> initialSource,
      int bucketId, int targetParallelism
  ) {
    this.currentSource = initialSource;
    this.index = -1;
  }

  @Override
  public boolean start() throws IOException {
    return advance();
  }

  @Override
  public boolean advance() throws IOException {

    return false;
  }

  @Override
  public Integer getCurrent() throws NoSuchElementException {
    if (next == null) {
      throw new NoSuchElementException();
    }
    return next.orNull();
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public BoundedSource<Integer> getCurrentSource() {
    return currentSource;
  }
}
