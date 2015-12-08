package com.lucidworks.hadoop.pig;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class EpochToCalendarTest {

  @Test
  public void testOneArg() throws Exception {
    EpochToCalendar test = new EpochToCalendar();
    TupleFactory Maker = TupleFactory.getInstance();

    String[] strArray = new String[] { "EPOCH", "YEAR" };
    List strList = Arrays.asList(strArray);
    Tuple test1a = Maker.newTuple(strList);
    test1a.set(0, 1704067200000L);
    assertEquals(test1a.toString(), "(1704067200000,YEAR)");
    Tuple test1 = test.exec(test1a);
    assertEquals(test1.toString(), "(2024)");

    String[] strArray2 = new String[] { "EPOCH", "MINUTE" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2a = Maker.newTuple(strList2);
    test2a.set(0, -62167389300000L);
    assertEquals(test2a.toString(), "(-62167389300000,MINUTE)");
    Tuple test2 = test.exec(test2a);
    assertEquals(test2.toString(), "(45)");
  }

  @Test
  public void testTwoArg() throws Exception {
    EpochToCalendar test = new EpochToCalendar();
    TupleFactory Maker = TupleFactory.getInstance();

    String[] strArray = new String[] { "EPOCH", "YEAR", "HOUR" };
    List strList = Arrays.asList(strArray);
    Tuple test1a = Maker.newTuple(strList);
    test1a.set(0, 1325433600000L);
    assertEquals(test1a.toString(), "(1325433600000,YEAR,HOUR)");
    Tuple test1 = test.exec(test1a);
    assertEquals(test1.toString(), "(2012,16)");

    String[] strArray2 = new String[] { "EPOCH", "MINUTE", "SECOND" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2a = Maker.newTuple(strList2);
    test2a.set(0, -62167389287000L);
    assertEquals(test2a.toString(), "(-62167389287000,MINUTE,SECOND)");
    Tuple test2 = test.exec(test2a);
    assertEquals(test2.toString(), "(45,13)");
  }

  @Test
  public void testThreeArg() throws Exception {
    EpochToCalendar test = new EpochToCalendar();
    TupleFactory Maker = TupleFactory.getInstance();

    String[] strArray = new String[] { "EPOCH", "YEAR", "MONTH", "DATE" };
    List strList = Arrays.asList(strArray);
    Tuple test1a = Maker.newTuple(strList);
    test1a.set(0, 1325376000000L);
    assertEquals(test1a.toString(), "(1325376000000,YEAR,MONTH,DATE)");
    Tuple test1 = test.exec(test1a);
    assertEquals(test1.toString(), "(2012,0,1)");

    String[] strArray2 = new String[] { "EPOCH", "MINUTE", "YEAR", "MILLISECOND" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2a = Maker.newTuple(strList2);
    test2a.set(0, 946687500013L);
    assertEquals(test2a.toString(), "(946687500013,MINUTE,YEAR,MILLISECOND)");
    Tuple test2 = test.exec(test2a);
    assertEquals(test2.toString(), "(45,2000,13)");
  }

  @Test
  public void testFourArg() throws Exception {
    EpochToCalendar test = new EpochToCalendar();
    TupleFactory Maker = TupleFactory.getInstance();

    String[] strArray = new String[] { "EPOCH", "YEAR", "MINUTE", "DATE", "MONTH" };
    List strList = Arrays.asList(strArray);
    Tuple test1a = Maker.newTuple(strList);
    test1a.set(0, 1349311980000L);
    assertEquals(test1a.toString(), "(1349311980000,YEAR,MINUTE,DATE,MONTH)");
    Tuple test1 = test.exec(test1a);
    assertEquals(test1.toString(), "(2012,53,4,9)");

    String[] strArray2 = new String[] { "EPOCH", "HOUR", "YEAR", "MILLISECOND", "SECOND" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2a = Maker.newTuple(strList2);
    test2a.set(0, 915310850015L);
    assertEquals(test2a.toString(), "(915310850015,HOUR,YEAR,MILLISECOND,SECOND)");
    Tuple test2 = test.exec(test2a);
    assertEquals(test2.toString(), "(21,1999,15,50)");
  }

  @Test
  public void testFiveArg() throws Exception {
    EpochToCalendar test = new EpochToCalendar();
    TupleFactory Maker = TupleFactory.getInstance();

    String[] strArray = new String[] { "EPOCH", "YEAR", "MINUTE", "DATE", "MONTH", "HOUR" };
    List strList = Arrays.asList(strArray);
    Tuple test1a = Maker.newTuple(strList);
    test1a.set(0, 684175980000L);
    assertEquals(test1a.toString(), "(684175980000,YEAR,MINUTE,DATE,MONTH,HOUR)");
    Tuple test1 = test.exec(test1a);
    assertEquals(test1.toString(), "(1991,53,6,8,16)");

    String[] strArray2 = new String[] { "EPOCH", "HOUR", "YEAR", "MILLISECOND", "SECOND", "MINUTE" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2a = Maker.newTuple(strList2);
    test2a.set(0, 915313370015L);
    assertEquals(test2a.toString(), "(915313370015,HOUR,YEAR,MILLISECOND,SECOND,MINUTE)");
    Tuple test2 = test.exec(test2a);
    assertEquals(test2.toString(), "(21,1999,15,50,42)");
  }

  @Test
  public void testSixArg() throws Exception {
    EpochToCalendar test = new EpochToCalendar();
    TupleFactory Maker = TupleFactory.getInstance();

    String[] strArray = new String[] { "EPOCH", "YEAR", "MINUTE", "DATE", "MONTH", "HOUR", "MILLISECOND" };
    List strList = Arrays.asList(strArray);
    Tuple test1a = Maker.newTuple(strList);
    test1a.set(0, 5838780043L);
    assertEquals(test1a.toString(), "(5838780043,YEAR,MINUTE,DATE,MONTH,HOUR,MILLISECOND)");
    Tuple test1 = test.exec(test1a);
    assertEquals(test1.toString(), "(1970,53,9,2,13,43)");

    String[] strArray2 = new String[] { "EPOCH", "HOUR", "YEAR", "MILLISECOND", "SECOND", "MINUTE", "MONTH" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2a = Maker.newTuple(strList2);
    test2a.set(0, 423358952015L);
    assertEquals(test2a.toString(), "(423358952015,HOUR,YEAR,MILLISECOND,SECOND,MINUTE,MONTH)");
    Tuple test2 = test.exec(test2a);
    assertEquals(test2.toString(), "(23,1983,15,32,42,5)");
  }

  @Test
  public void testSevenArg() throws Exception {
    EpochToCalendar test = new EpochToCalendar();
    TupleFactory Maker = TupleFactory.getInstance();

    String[] strArray = new String[] { "EPOCH", "YEAR", "MINUTE", "DATE", "MONTH", "HOUR", "MILLISECOND", "SECOND" };
    List strList = Arrays.asList(strArray);
    Tuple test1a = Maker.newTuple(strList);
    test1a.set(0, 5838804043L);
    assertEquals(test1a.toString(), "(5838804043,YEAR,MINUTE,DATE,MONTH,HOUR,MILLISECOND,SECOND)");
    Tuple test1 = test.exec(test1a);
    assertEquals(test1.toString(), "(1970,53,9,2,13,43,24)");

    String[] strArray2 = new String[] { "EPOCH", "HOUR", "YEAR", "MILLISECOND", "SECOND", "MINUTE", "MONTH", "DATE" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2a = Maker.newTuple(strList2);
    test2a.set(0, 423618152015L);
    assertEquals(test2a.toString(), "(423618152015,HOUR,YEAR,MILLISECOND,SECOND,MINUTE,MONTH,DATE)");
    Tuple test2 = test.exec(test2a);
    assertEquals(test2.toString(), "(23,1983,15,32,42,5,4)");
  }
}
