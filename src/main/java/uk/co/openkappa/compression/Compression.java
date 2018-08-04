package uk.co.openkappa.compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

interface Compression<T> {
  String name();
  int bytes(int count, IntFunction<Stream<T>> in, Function<T, byte[]> serialiser);
}

interface Encoding<T> extends Function<T, byte[]> {
  static <T> Encoding<T> of(String name, Function<T, byte[]> serialiser) {
    return new Encoding<>() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public byte[] apply(T value) {
        return serialiser.apply(value);
      }
    };
  }

  String name();
}

class LZ4<T> implements Compression<T> {

  private final int capacity;

  LZ4(int capacity) {
    this.capacity = capacity;
  }

  @Override
  public String name() {
    return "LZ4";
  }

  @Override
  public int bytes(int count, IntFunction<Stream<T>> gen, Function<T, byte[]> serialiser) {
    LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ4Compressor compressor = factory.fastCompressor();
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    gen.apply(count).map(serialiser).forEach(buffer::put);
    ByteBuffer compressed = ByteBuffer.allocate(capacity);
    compressor.compress(buffer.flip(), compressed);
    return compressed.position();
  }
}

class Snappy<T> implements Compression<T> {

  private final int capacity;

  Snappy(int capacity) {
    this.capacity = capacity;
  }

  @Override
  public String name() {
    return "Snappy";
  }

  @Override
  public int bytes(int count, IntFunction<Stream<T>> gen, Function<T, byte[]> serialiser) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
    ByteBuffer compressed = ByteBuffer.allocateDirect(capacity);
    gen.apply(count).map(serialiser).forEach(buffer::put);
    try {
      org.xerial.snappy.Snappy.compress(buffer.flip(), compressed);
      return compressed.limit();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

class Uncompressed<T> implements Compression<T> {

  @Override
  public String name() {
    return "Uncompressed";
  }

  @Override
  public int bytes(int count, IntFunction<Stream<T>> gen, Function<T, byte[]> serialiser) {
    return gen.apply(count).map(serialiser).mapToInt(b -> b.length).sum();
  }
}

class Gzip<T> implements Compression<T> {

  @Override
  public String name() {
    return "GZIP";
  }

  @Override
  public int bytes(int count, IntFunction<Stream<T>> gen, Function<T, byte[]> serialiser) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(256 * 1024 * 1024);
    try (GZIPOutputStream gzos = new GZIPOutputStream(bos)) {
      Iterable<byte[]> data = () -> gen.apply(count).map(serialiser).iterator();
      for (byte[] d : data) {
        gzos.write(d);
      }
      gzos.finish();
      return bos.size();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

@FunctionalInterface
interface Throws<T, U, E extends Exception> {
  U apply(T value) throws E;
}

final class Util {

  static byte[] base64(byte[] input) {
    return Base64.getEncoder().encode(input);
  }

  static Stream<byte[]> randomKiloBytes(int count) {
    return gen(count, random);
  }

  static Stream<byte[]> monotonicKiloBytes(int count) {
    return gen(count, monotonic);
  }

  static Stream<byte[]> gen(int count, IntFunction<byte[]> map) {
    return IntStream.range(0, count).mapToObj(map);
  }

  static final IntFunction<byte[]> random = i -> {
    byte[] kb = new byte[1024];
    ThreadLocalRandom.current().nextBytes(kb);
    return kb;
  };

  static final IntFunction<byte[]> monotonic = i -> {
    byte[] kb = new byte[1024];
    int from = i * 256;
    for (int j = 0; j < 1024; j += 4) {
      kb[j] = (byte)from;
      kb[j + 1] = (byte)(from >>> 8);
      kb[j + 2] = (byte)(from >>> 16);
      kb[j + 3] = (byte)(from >>> 24);
      ++from;
    }
    return kb;
  };

  static <T, U, E extends Exception> Function<T, U> unchecked(Throws<T, U, E> unsafe) {
    return t -> {
      try {
        return unsafe.apply(t);
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }
    };
  }

  static final Function<byte[], byte[]> snappy = unchecked(org.xerial.snappy.Snappy::compress);

  static final Function<byte[], byte[]> lz4 = LZ4Factory.fastestInstance().highCompressor()::compress;

}