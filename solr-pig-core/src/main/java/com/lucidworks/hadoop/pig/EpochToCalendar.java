package com.lucidworks.hadoop.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Converts an Epoch timestamp to a tuple of Calendar fields
 * <p/>
 * Example:
 * <p/>
 * > EpochToCalendar(1325376000000, 'YEAR', 'MONTH', 'DATE');
 * < (2012, 0, 1)
 * <p/>
 * All standard date and time fields that Calendar supports are supported as
 * well as a few non-standard ones (DAY_OF_WEEK, etc)
 *
 * @see java.util.Calendar
 */
public class EpochToCalendar extends EvalFunc<Tuple> {

  private static final Map<String, Integer> calMap = new HashMap<String, Integer>();

  {
    calMap.put("YEAR", Calendar.YEAR);
    calMap.put("MONTH", Calendar.MONTH);
    calMap.put("DATE", Calendar.DATE);
    calMap.put("HOUR", Calendar.HOUR_OF_DAY);
    calMap.put("MINUTE", Calendar.MINUTE);
    calMap.put("SECOND", Calendar.SECOND);
    calMap.put("MILLISECOND", Calendar.MILLISECOND);
    calMap.put("DAY_OF_WEEK", Calendar.DAY_OF_WEEK);
    calMap.put("DAY_OF_MONTH", Calendar.DAY_OF_MONTH);
    calMap.put("DAY_OF_YEAR", Calendar.DAY_OF_YEAR);
    calMap.put("WEEK_OF_MONTH", Calendar.WEEK_OF_MONTH);
    calMap.put("WEEK_OF_YEAR", Calendar.WEEK_OF_YEAR);
  }

  ;

  @Override
  public Tuple exec(Tuple tuple) throws IOException {
    if (tuple == null || tuple.size() < 2) {
      throw new IOException("Not enough values");
    }
    long epoch = (Long) tuple.get(0);
    Tuple out = TupleFactory.getInstance().newTuple(tuple.size() - 1);
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTimeInMillis(epoch);
    for (int i = 1; i < tuple.size(); i++) {
      String key = (String) tuple.get(i);
      Integer calKey = calMap.get(key);
      if (calKey == null) {
        out.set(i - 1, null);
      } else {
        out.set(i - 1, cal.get(calKey));
      }
    }
    return out;
  }

}
