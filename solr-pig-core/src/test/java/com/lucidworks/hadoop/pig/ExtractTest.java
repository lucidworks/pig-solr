package com.lucidworks.hadoop.pig;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ExtractTest {

  @Test
  public void testSimple() throws IOException {
    Extract extractor = new Extract();
    TupleFactory maker = TupleFactory.getInstance();
    DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();

    int[] values = new int[] { 6, 22 };
    String[] strings = new String[] { "term1", "term2" };
    Tuple[] tuples = new Tuple[2];
    for (int i = 0; i < tuples.length; i++) {
      tuples[i] = maker.newTuple(Arrays.asList(strings[i], values[i]));
      bag.add(tuples[i]);
    }

    Tuple test1 = maker.newTuple(Arrays.asList(bag, 0));
    Tuple result1 = extractor.exec(test1);
    for (int i = 0; i < tuples.length; i++) {
      assertEquals(result1.get(i), strings[i]);
    }

    Tuple test2 = maker.newTuple(Arrays.asList(bag, 1));
    Tuple result2 = extractor.exec(test2);
    for (int i = 0; i < tuples.length; i++) {
      assertEquals(result2.get(i), values[i]);
    }
  }

  @Test
  public void testLongTuple() throws IOException {
    Extract extractor = new Extract();
    TupleFactory maker = TupleFactory.getInstance();
    DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();

    Integer[] values = new Integer[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    Integer[] values2 = new Integer[] { 0, 11, 22, 33, 44, 55, 66, 77, 88, 99, 1010, 1111, 1212, 1313, 1414, 1515 };
    String[] strings = new String[] { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
            "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen" };
    Object[][] arrays = new Object[][] { values, values2, strings };
    Tuple[] tuples = new Tuple[3];
    for (int i = 0; i < tuples.length; i++) {
      tuples[i] = maker.newTuple();
      bag.add(tuples[i]);
    }
    for (int i = 0; i < values.length; i++) {
      tuples[0].append(values[i]);
      tuples[1].append(values2[i]);
      tuples[2].append(strings[i]);
    }

    Tuple test1 = maker.newTuple(Arrays.asList(bag, 0));
    Tuple result1 = extractor.exec(test1);
    for (int i = 0; i < arrays.length; i++) {
      assertEquals(result1.get(i), (arrays[i][0]));
    }

    Tuple test2 = maker.newTuple(Arrays.asList(bag, 3));
    Tuple result2 = extractor.exec(test2);
    for (int i = 0; i < arrays.length; i++) {
      assertEquals(result2.get(i), (arrays[i][3]));
    }

    Tuple test3 = maker.newTuple(Arrays.asList(bag, 8));
    Tuple result3 = extractor.exec(test3);
    for (int i = 0; i < arrays.length; i++) {
      assertEquals(result3.get(i), (arrays[i][8]));
    }

    Tuple test4 = maker.newTuple(Arrays.asList(bag, 15));
    Tuple result4 = extractor.exec(test4);
    for (int i = 0; i < arrays.length; i++) {
      assertEquals(result4.get(i), (arrays[i][15]));
    }
  }

  @Test
  public void testLongBag() throws IOException {
    Extract extractor = new Extract();
    TupleFactory maker = TupleFactory.getInstance();
    DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();

    int[] values = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    String[] strings = new String[] { "term0", "term1", "term2", "term3", "term4", "term5", "term6", "term7", "term8",
            "term9", "term10" };
    Tuple[] tuples = new Tuple[11];
    for (int i = 0; i < tuples.length; i++) {
      tuples[i] = maker.newTuple(Arrays.asList(strings[i], values[i]));
      bag.add(tuples[i]);
    }

    Tuple test1 = maker.newTuple(Arrays.asList(bag, 0));
    Tuple result1 = extractor.exec(test1);
    for (int i = 0; i < tuples.length; i++) {
      assertEquals(result1.get(i), strings[i]);
    }

    Tuple test2 = maker.newTuple(Arrays.asList(bag, 1));
    Tuple result2 = extractor.exec(test2);
    for (int i = 0; i < tuples.length; i++) {
      assertEquals(result2.get(i), values[i]);
    }
  }

  @Test
  public void testLongBagLongTuple() throws IOException {
    Extract extractor = new Extract();
    TupleFactory maker = TupleFactory.getInstance();
    DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();

    Integer[] values = new Integer[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    Integer[] values2 = new Integer[] { 0, 11, 22, 33, 44, 55, 66, 77, 88, 99, 1010, 1111, 1212, 1313, 1414, 1515 };
    String[] strings = new String[] { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
            "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen" };
    String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p" };
    Object[][] arrays = new Object[][] { values, values2, strings, letters };
    Tuple[] tuples = new Tuple[12];
    for (int i = 0; i < tuples.length; i++) {
      tuples[i] = maker.newTuple();
      bag.add(tuples[i]);
    }
    for (int j = 1; j < 4; j++) {
      for (int i = 0; i < values.length; i++) {
        tuples[4 * j - 4].append(values[i]);
        tuples[4 * j - 3].append(values2[i]);
        tuples[4 * j - 2].append(strings[i]);
        tuples[4 * j - 1].append(letters[i]);
      }
    }

    Tuple test1 = maker.newTuple(Arrays.asList(bag, 0));
    Tuple result1 = extractor.exec(test1);
    for (int i = 0; i < arrays.length; i++) {
      assertEquals(result1.get(i), (arrays[i][0]));
    }

    Tuple test2 = maker.newTuple(Arrays.asList(bag, 3));
    Tuple result2 = extractor.exec(test2);
    for (int i = 0; i < arrays.length; i++) {
      assertEquals(result2.get(i), (arrays[i][3]));
    }

    Tuple test3 = maker.newTuple(Arrays.asList(bag, 8));
    Tuple result3 = extractor.exec(test3);
    for (int i = 0; i < arrays.length; i++) {
      assertEquals(result3.get(i), (arrays[i][8]));
    }

    Tuple test4 = maker.newTuple(Arrays.asList(bag, 15));
    Tuple result4 = extractor.exec(test4);
    for (int i = 0; i < arrays.length; i++) {
      assertEquals(result4.get(i), (arrays[i][15]));
    }
  }
}

