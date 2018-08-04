package uk.co.openkappa.compression;

import java.util.function.Function;

import static java.util.Arrays.asList;
import static uk.co.openkappa.compression.CompressionBenchmark.benchmark;

public class ComposedCompression {


  public static void main(String[] args) {
    var preCompressions = asList(
            Encoding.of("Uncompressed", Function.identity()),
            Encoding.of("Snappy", Util.snappy),
            Encoding.of("LZ4", Util.lz4));
    var compressions = asList(
            new Uncompressed<byte[]>(),
            new LZ4<byte[]>( 1536 * 1024 * 1024),
            new Snappy<byte[]>(1536 * 1024 * 1024),
            new Gzip<byte[]>()
    );
    //benchmark(preCompressions, compressions, Util::randomKiloBytes, 1_000_000);
    benchmark(preCompressions, compressions, Util::monotonicKiloBytes, 1_000_000);
  }
}
