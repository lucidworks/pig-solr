package com.lucidworks.hadoop.pig;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CalendarToEpochTest {

  @Test
  public void testOneArg() throws Exception {
    CalendarToEpoch test = new CalendarToEpoch();
    TupleFactory maker = TupleFactory.getInstance();

    Tuple test1a = maker.newTuple(Integer.valueOf(2024));
    String[] strArray = new String[] { "TIMETUPLE", "YEAR" };
    List strList = Arrays.asList(strArray);
    Tuple test1b = maker.newTuple(strList);
    test1b.set(0, test1a);
    assertEquals(test1b.toString(), "((2024),YEAR)");
    Long test1 = test.exec(test1b);
    assertEquals((long) test1, 1704067200000L);

    Tuple test2a = maker.newTuple(Integer.valueOf(45));
    String[] strArray2 = new String[] { "TIMETUPLE", "MINUTE" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2b = maker.newTuple(strList2);
    test2b.set(0, test2a);
    assertEquals(test2b.toString(), "((45),MINUTE)");
    Long test2 = test.exec(test2b);
    assertEquals((long) test2, -62167389300000L);
  }

  @Test
  public void testTwoArg() throws Exception {
    CalendarToEpoch test = new CalendarToEpoch();
    TupleFactory maker = TupleFactory.getInstance();

    Integer[] numArray = new Integer[] { 2012, 16 };
    Tuple test1a = maker.newTuple(Arrays.asList(numArray));
    String[] strArray = new String[] { "TIMETUPLE", "YEAR", "HOUR" };
    List strList = Arrays.asList(strArray);
    Tuple test1b = maker.newTuple(strList);
    test1b.set(0, test1a);
    assertEquals(test1b.toString(), "((2012,16),YEAR,HOUR)");
    Long test1 = test.exec(test1b);
    assertEquals((long) test1, 1325433600000L);

    Integer[] numArray2 = new Integer[] { 45, 13 };
    Tuple test2a = maker.newTuple(Arrays.asList(numArray2));
    String[] strArray2 = new String[] { "TIMETUPLE", "MINUTE", "SECOND" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2b = maker.newTuple(strList2);
    test2b.set(0, test2a);
    assertEquals(test2b.toString(), "((45,13),MINUTE,SECOND)");
    Long test2 = test.exec(test2b);
    assertEquals((long) test2, -62167389287000L);
  }

  @Test
  public void testThreeArg() throws Exception {
    CalendarToEpoch test = new CalendarToEpoch();
    TupleFactory maker = TupleFactory.getInstance();

    Integer[] numArray = new Integer[] { 2012, 0, 1 };
    Tuple test1a = maker.newTuple(Arrays.asList(numArray));
    String[] strArray = new String[] { "TIMETUPLE", "YEAR", "MONTH", "DATE" };
    List strList = Arrays.asList(strArray);
    Tuple test1b = maker.newTuple(strList);
    test1b.set(0, test1a);
    assertEquals(test1b.toString(), "((2012,0,1),YEAR,MONTH,DATE)");
    Long test1 = test.exec(test1b);
    assertEquals((long) test1, 1325376000000L);

    Integer[] numArray2 = new Integer[] { 45, 2000, 13 };
    Tuple test2a = maker.newTuple(Arrays.asList(numArray2));
    String[] strArray2 = new String[] { "TIMETUPLE", "MINUTE", "YEAR", "MILLISECOND" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2b = maker.newTuple(strList2);
    test2b.set(0, test2a);
    assertEquals(test2b.toString(), "((45,2000,13),MINUTE,YEAR,MILLISECOND)");
    Long test2 = test.exec(test2b);
    assertEquals((long) test2, 946687500013L);
  }

  @Test
  public void testFourArg() throws Exception {
    CalendarToEpoch test = new CalendarToEpoch();
    TupleFactory maker = TupleFactory.getInstance();

    Integer[] numArray = new Integer[] { 2012, 53, 4, 9 };
    Tuple test1a = maker.newTuple(Arrays.asList(numArray));
    String[] strArray = new String[] { "TIMETUPLE", "YEAR", "MINUTE", "DATE", "MONTH" };
    List strList = Arrays.asList(strArray);
    Tuple test1b = maker.newTuple(strList);
    test1b.set(0, test1a);
    assertEquals(test1b.toString(), "((2012,53,4,9),YEAR,MINUTE,DATE,MONTH)");
    Long test1 = test.exec(test1b);
    assertEquals((long) test1, 1349311980000L);

    Integer[] numArray2 = new Integer[] { 45, 1999, 15, 50 };
    Tuple test2a = maker.newTuple(Arrays.asList(numArray2));
    String[] strArray2 = new String[] { "TIMETUPLE", "HOUR", "YEAR", "MILLISECOND", "SECOND" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2b = maker.newTuple(strList2);
    test2b.set(0, test2a);
    assertEquals(test2b.toString(), "((45,1999,15,50),HOUR,YEAR,MILLISECOND,SECOND)");
    Long test2 = test.exec(test2b);
    assertEquals((long) test2, 915310850015L);
  }

  @Test
  public void testFiveArg() throws Exception {
    CalendarToEpoch test = new CalendarToEpoch();
    TupleFactory maker = TupleFactory.getInstance();

    Integer[] numArray = new Integer[] { 1991, 53, 6, 8, 16 };
    Tuple test1a = maker.newTuple(Arrays.asList(numArray));
    String[] strArray = new String[] { "TIMETUPLE", "YEAR", "MINUTE", "DATE", "MONTH", "HOUR" };
    List strList = Arrays.asList(strArray);
    Tuple test1b = maker.newTuple(strList);
    test1b.set(0, test1a);
    assertEquals(test1b.toString(), "((1991,53,6,8,16),YEAR,MINUTE,DATE,MONTH,HOUR)");
    Long test1 = test.exec(test1b);
    assertEquals((long) test1, 684175980000L);

    Integer[] numArray2 = new Integer[] { 45, 1999, 15, 50, 42 };
    Tuple test2a = maker.newTuple(Arrays.asList(numArray2));
    String[] strArray2 = new String[] { "TIMETUPLE", "HOUR", "YEAR", "MILLISECOND", "SECOND", "MINUTE" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2b = maker.newTuple(strList2);
    test2b.set(0, test2a);
    assertEquals(test2b.toString(), "((45,1999,15,50,42),HOUR,YEAR,MILLISECOND,SECOND,MINUTE)");
    Long test2 = test.exec(test2b);
    assertEquals((long) test2, 915313370015L);
  }

  @Test
  public void testSixArg() throws Exception {
    CalendarToEpoch test = new CalendarToEpoch();
    TupleFactory maker = TupleFactory.getInstance();

    Integer[] numArray = new Integer[] { 1970, 53, 9, 2, 13, 43 };
    Tuple test1a = maker.newTuple(Arrays.asList(numArray));
    String[] strArray = new String[] { "TIMETUPLE", "YEAR", "MINUTE", "DATE", "MONTH", "HOUR", "MILLISECOND" };
    List strList = Arrays.asList(strArray);
    Tuple test1b = maker.newTuple(strList);
    test1b.set(0, test1a);
    assertEquals(test1b.toString(), "((1970,53,9,2,13,43),YEAR,MINUTE,DATE,MONTH,HOUR,MILLISECOND)");
    Long test1 = test.exec(test1b);
    assertEquals((long) test1, 5838780043L);

    Integer[] numArray2 = new Integer[] { 23, 1983, 15, 32, 42, 5 };
    Tuple test2a = maker.newTuple(Arrays.asList(numArray2));
    String[] strArray2 = new String[] { "TIMETUPLE", "HOUR", "YEAR", "MILLISECOND", "SECOND", "MINUTE", "MONTH" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2b = maker.newTuple(strList2);
    test2b.set(0, test2a);
    assertEquals(test2b.toString(), "((23,1983,15,32,42,5),HOUR,YEAR,MILLISECOND,SECOND,MINUTE,MONTH)");
    Long test2 = test.exec(test2b);
    assertEquals((long) test2, 423358952015L);
  }

  @Test
  public void testSevenArg() throws Exception {
    CalendarToEpoch test = new CalendarToEpoch();
    TupleFactory maker = TupleFactory.getInstance();

    Integer[] numArray = new Integer[] { 1970, 53, 9, 2, 13, 43, 24 };
    Tuple test1a = maker.newTuple(Arrays.asList(numArray));
    String[] strArray = new String[] { "TIMETUPLE", "YEAR", "MINUTE", "DATE", "MONTH", "HOUR", "MILLISECOND",
            "SECOND" };
    List strList = Arrays.asList(strArray);
    Tuple test1b = maker.newTuple(strList);
    test1b.set(0, test1a);
    assertEquals(test1b.toString(), "((1970,53,9,2,13,43,24),YEAR,MINUTE,DATE,MONTH,HOUR,MILLISECOND,SECOND)");
    Long test1 = test.exec(test1b);
    assertEquals((long) test1, 5838804043L);

    Integer[] numArray2 = new Integer[] { 23, 1983, 15, 32, 42, 5, 4 };
    Tuple test2a = maker.newTuple(Arrays.asList(numArray2));
    String[] strArray2 = new String[] { "TIMETUPLE", "HOUR", "YEAR", "MILLISECOND", "SECOND", "MINUTE", "MONTH",
            "DATE" };
    List strList2 = Arrays.asList(strArray2);
    Tuple test2b = maker.newTuple(strList2);
    test2b.set(0, test2a);
    assertEquals(test2b.toString(), "((23,1983,15,32,42,5,4),HOUR,YEAR,MILLISECOND,SECOND,MINUTE,MONTH,DATE)");
    Long test2 = test.exec(test2b);
    assertEquals((long) test2, 423618152015L);
  }
}
