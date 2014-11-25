import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TestHashMap {

  public final int SET_SIZE = 10000000;
  public final int DATA_SIZE = 1000000;

  @Test
  public void testHashSetString() throws Exception {
    final Set<String> hashSet = new HashSet<>();
    final Random random = new Random(0xDEADBEEF);
    int matched = 0;

    long startTime = System.nanoTime();

    for (int i = 0; i < SET_SIZE; i++) {
      // input data is String
      String num = Long.toString(random.nextLong());
      hashSet.add(num);
    }

    for (int i = 0; i < DATA_SIZE; i++) {
      // query data is long
      long num = random.nextLong();
      if (hashSet.contains(Long.toString(num))) {
        matched++;
      }
    }

    long endTime = System.nanoTime();
    System.out.println("  HashSet<String>");
    System.out.println("  Elapsed time: " + (endTime - startTime) / 1000000 + " ms");
    System.out.println("  Matched " + matched + " times");
  }

  @Test
  public void testHashSetLongPrimitive() throws Exception {
    final Set<Long> hashSet = new HashSet<>();
    final Random random = new Random(0xDEADBEEF);
    int matched = 0;

    long startTime = System.nanoTime();

    for (int i = 0; i < SET_SIZE; i++) {
      // input data is string
      String num = Long.toString(random.nextLong());
      hashSet.add(Long.parseLong(num));
    }

    for (int i = 0; i < DATA_SIZE; i++) {
      // query data is long
      long num = random.nextLong();
      if (hashSet.contains(num)) {
        matched++;
      }
    }

    long endTime = System.nanoTime();
    System.out.println("  HashSet<long>");
    System.out.println("  Elapsed time: " + (endTime - startTime) / 1000000f + " ms");
    System.out.println("  Matched " + matched + " times");
  }

  @Test
  public void testHashSetLongClass() throws Exception {
    final Set<Long> hashSet = new HashSet<>();
    final Random random = new Random(0xDEADBEEF);
    int matched = 0;

    long startTime = System.nanoTime();

    for (int i = 0; i < SET_SIZE; i++) {
      // input data is string
      String num = Long.toString(random.nextLong());
      hashSet.add(Long.getLong(num));
    }

    for (int i = 0; i < DATA_SIZE; i++) {
      // query data is long
      Long num = random.nextLong();
      if (hashSet.contains(num)) {
        matched++;
      }
    }

    long endTime = System.nanoTime();
    System.out.println("  HashSet<Long>");
    System.out.println("  Elapsed time: " + (endTime - startTime) / 1000000f + " ms");
    System.out.println("  Matched " + matched + " times");
  }
}
