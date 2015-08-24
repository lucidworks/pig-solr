package com.lucidworks.hadoop.pig;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class TestHistogram {

  @Test
  public void testPigExec() throws IOException {
    Histogram hist = new Histogram();
    DataBag bag = numberArrayToDataBag(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    Tuple args = TupleFactory.getInstance().newTuple(Arrays.asList(bag, 1, 10, 2));
    Tuple result = hist.exec(args);
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), 5L);
    Assert.assertEquals(result.get(1), 5L);
  }

  @Test
  public void testArgsExec() throws IOException {
    Histogram hist = new Histogram();
    DataBag bag = numberArrayToDataBag(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    Tuple result = hist.exec(bag, 1, 10, 2);
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), 5L);
    Assert.assertEquals(result.get(1), 5L);
  }

  @Test
  public void testUniformDistribution() throws IOException {
    Random rnd = getRandomInstance();
    for (int i = 0; i < 100; i++) {
      DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
      for (int j = 0; j < 100; j++) {
        bag.add(TupleFactory.getInstance().newTuple(new Integer(rnd.nextInt(100))));
      }
      Tuple result = Histogram.exec(bag, 0, 99, 1);
      //System.err.println(result);
      Assert.assertEquals(result.size(), 1);
      Assert.assertEquals(result.get(0), 100L);

      result = Histogram.exec(bag, 0, 99, 2);
      //System.err.println(result);
      Assert.assertEquals(result.size(), 2);

      result = Histogram.exec(bag, 0, 99, 10);
      //System.err.println(result);
      Assert.assertEquals(result.size(), 10);
    }
  }

  @Test
  public void testNormalDistribution() throws IOException {
    Random rnd = getRandomInstance();
    for (int i = 0; i < 1000; i++) {
      DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
      for (int j = 0; j < 100; j++) {
        int x = Math.max(Math.min((int) (50 + rnd.nextGaussian() * 50), 100), 0);
        bag.add(TupleFactory.getInstance().newTuple(new Integer(x)));
      }
      Tuple result = Histogram.exec(bag, 0, 100, 1);
      //System.err.println(result);
      Assert.assertEquals(result.size(), 1);
      Assert.assertEquals(result.get(0), 100L);

      result = Histogram.exec(bag, 0, 100, 2);
      //System.err.println(result);
      Assert.assertEquals(result.size(), 2);

      result = Histogram.exec(bag, 0, 100, 10);
      //System.err.println(result);
      Assert.assertEquals(result.size(), 10);
    }
  }

  @Test
  public void testOnePoint() throws IOException {
    DataBag bag = numberArrayToDataBag(1);
    Tuple result = Histogram.exec(bag, 1, 1, 1);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 1L);
    result = Histogram.exec(bag, 1, 1, 2);
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), 1L);
    result = Histogram.exec(bag, 1, 1, 100);
    Assert.assertEquals(result.size(), 100);
    Assert.assertEquals(result.get(0), 1L);
  }

  @Test
  public void testSpikeyDistribution() throws IOException {
    DataBag bag = numberArrayToDataBag(1, 1, 1, 1, 1, 1, 1);
    Tuple result = Histogram.exec(bag, 1, 1, 1);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 7L);
    result = Histogram.exec(bag, 1, 1, 2);
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), 7L);
    result = Histogram.exec(bag, 1, 1, 4);
    Assert.assertEquals(result.size(), 4);
    Assert.assertEquals(result.get(0), 7L);
    result = Histogram.exec(bag, 1, 1, 8);
    Assert.assertEquals(result.size(), 8);
    Assert.assertEquals(result.get(0), 7L);
    result = Histogram.exec(bag, 1, 1, 16);
    Assert.assertEquals(result.size(), 16);
    Assert.assertEquals(result.get(0), 7L);

    bag = numberArrayToDataBag(1, 1, 1, 1, 1, 1, 2);
    result = Histogram.exec(bag, 1, 2, 1);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 7L);
    result = Histogram.exec(bag, 1, 2, 2);
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), 6L);
    Assert.assertEquals(result.get(1), 1L);
  }

  @Test
  public void testWidth() throws IOException {
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 1), 10);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 2), 5);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 3), 4);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 4), 3);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 5), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 6), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 7), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 8), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 9), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 10), 1);
    Assert.assertEquals(Histogram.calculateWidth(1, 10, 11), 1);

    Assert.assertEquals(Histogram.calculateWidth(1, 12, 1), 12);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 2), 6);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 3), 4);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 4), 3);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 5), 3);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 6), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 7), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 8), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 9), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 10), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 11), 2);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 12), 1);
    Assert.assertEquals(Histogram.calculateWidth(1, 12, 13), 1);

    Assert.assertEquals(Histogram.calculateWidth(1, 1, 1), 1);
    Assert.assertEquals(Histogram.calculateWidth(1, 1, 2), 1);

    try {
      Histogram.calculateWidth(1, 1, 0);
      Assert.fail("Should have gotten an IOException");
    } catch (IOException e) {
      Assert.assertTrue(true);
    }
  }

  private static DataBag numberArrayToDataBag(Number... values) {
    DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
    for (Number value : values) {
      bag.add(TupleFactory.getInstance().newTuple(value));
    }
    return bag;
  }

  private static Random getRandomInstance() {
    String buildNumber = System.getProperty("BUILD_NUMBER");
    Random rnd;
    if (buildNumber != null) {
      rnd = new Random(buildNumber.hashCode());
    } else {
      rnd = new Random();
      long seed = rnd.nextLong();
      System.err.println("Using random seed: " + seed);
      rnd = new Random(seed);
    }
    return rnd;
  }

}
