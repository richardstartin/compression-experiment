package uk.co.openkappa.compression;

import java.util.function.Function;

import static java.util.Arrays.asList;
import static uk.co.openkappa.compression.CompressionBenchmark.benchmark;
import static uk.co.openkappa.compression.Util.base64;
import static uk.co.openkappa.compression.Util.lz4;
import static uk.co.openkappa.compression.Util.snappy;

public class Base64PreCompression {

  public static void main(String[] args) {
    var encodings = asList(
            Encoding.of("binary", Function.identity()),
            Encoding.of("base64", base64),
            Encoding.of("base64/snappy", base64.andThen(snappy)),
            Encoding.of("base64/lz4", base64.andThen(lz4))
    );
    var compressions = asList(
            new Uncompressed<byte[]>(),
            new Gzip<byte[]>()
    );
    benchmark(encodings, compressions, Util::monotonicKiloBytes, 1_000_000);
    benchmark(encodings, compressions, Util::sinusoidalKiloBytes, 1_000_000);
  }
}
