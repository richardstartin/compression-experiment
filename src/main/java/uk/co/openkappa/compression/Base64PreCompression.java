package uk.co.openkappa.compression;

import java.util.function.Function;

import static java.util.Arrays.asList;
import static uk.co.openkappa.compression.CompressionBenchmark.benchmark;

public class Base64PreCompression {

  public static void main(String[] args) {
    var encodings = asList(
            Encoding.of("binary", Function.identity()),
            Encoding.of("base64", Util.base64),
            Encoding.of("base64/snappy", Util.base64.andThen(Util.snappy)),
            Encoding.of("base64/lz4", Util.base64.andThen(Util.lz4))
    );
    var compressions = asList(
            new Uncompressed<byte[]>(),
            new Gzip<byte[]>()
    );
    benchmark(encodings, compressions, Util::monotonicKiloBytes, 1_000_000);
  }
}
