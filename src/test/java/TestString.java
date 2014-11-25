import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestString {

  @Test
  public void testStringSubstring() throws Exception {
    Text text = new Text("string");
    Text text1 = new Text();
    Text text2 = new Text();

    long start = System.nanoTime();
    for (int i = 0; i < 100000000; i++) {
      String str = text.toString();
      String str1 = str.substring(0, 2);
      String str2 = str.substring(3, str.length());
      text1.set(str1);
      text2.set(str2);
    }
    long end = System.nanoTime();
    System.out.println("TextStringSubString");
    System.out.println("text1: " + text1.toString());
    System.out.println("text2: " + text2.toString());
    System.out.println("Elapsed Time: " + 
        (end - start) / 1000000000f + " seconds.");
  }

  @Test
  public void testTextSubstring() throws Exception {
    Text text = new Text("string");
    Text text1 = new Text();
    Text text2 = new Text();

    long start = System.nanoTime();
    for (int i = 0; i < 100000000; i++) {
      text1.set(text.getBytes(), 0, 2);
      text2.set(text.getBytes(), 3, text.getLength() - 3);
    }
    long end = System.nanoTime();
    System.out.println("TestTextSubString");
    System.out.println("text1: " + text1.toString());
    System.out.println("text2: " + text2.toString());
    System.out.println("Elapsed Time: " +
        (end - start) / 1000000000f + " seconds.");
  }
}
