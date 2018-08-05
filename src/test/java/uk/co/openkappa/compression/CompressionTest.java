package uk.co.openkappa.compression;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static uk.co.openkappa.compression.Util.monotonicKiloBytes;
import static uk.co.openkappa.compression.Util.unbase64;

public class CompressionTest {


  @Test
  public void testMonotonicKiloBytesStable() {
    var inv1 = monotonicKiloBytes(100_000).collect(toList());
    var inv2 = monotonicKiloBytes(100_000).collect(toList());
    assertEquals(inv1.size(), inv2.size());
    for (int i = 0; i < inv1.size(); ++i) {
      assertArrayEquals(inv1.get(i), inv2.get(i));
    }
  }

  @Test
  public void testSnappyBase64ComposedOutputRecoverable() throws IOException {
    int count = 100_000;
    byte[] gzipped = new Gzip<byte[]>().compress(count, Util::monotonicKiloBytes, Util.base64.andThen(Util.snappy));
    try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(gzipped))) {
      byte[] ungzipped = gzip.readAllBytes();
      byte[] shouldEqual = new byte[ungzipped.length];
      int[] offset = new int[1];
      var revert = Util.unsnappy.andThen(unbase64);
      Util.monotonicKiloBytes(count)
          .map(Util.base64.andThen(Util.snappy))
          .forEach(arr -> {
            int start = offset[0];
            System.arraycopy(arr, 0, shouldEqual, start, arr.length);
            assertArrayEquals(revert.apply(arr), revert.apply(Arrays.copyOfRange(ungzipped, start, start + arr.length)));
            offset[0] += arr.length;
          });
      assertArrayEquals(shouldEqual, ungzipped);
    }
  }


}