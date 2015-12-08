package com.lucidworks.hadoop.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Converts a tuple of Calendar fields to a Epoch timestamp
 * <p/>
 * Example:
 * <p/>
 * > CalendarToEpoch((2012, 0, 1), 'YEAR', 'MONTH', 'DATE');
 * < 1325376000000
 * <p/>
 * Unspecified fields are set to zero. Only standard date and time fields are
 * supported (so no DAY_OF_WEEK, WEEK_OF_MONTH, etc).
 *
 * @see java.util.Calendar
 */
public class CalendarToEpoch extends EvalFunc<Long> {

  private static final Map<String, Integer> calMap = new HashMap<String, Integer>();
  private static final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  {
    // field map
    calMap.put("YEAR", Calendar.YEAR);
    calMap.put("MONTH", Calendar.MONTH);
    calMap.put("DATE", Calendar.DATE);
    calMap.put("HOUR", Calendar.HOUR_OF_DAY);
    calMap.put("MINUTE", Calendar.MINUTE);
    calMap.put("SECOND", Calendar.SECOND);
    calMap.put("MILLISECOND", Calendar.MILLISECOND);
    // zero out fields
    cal.set(Calendar.YEAR, 0);
    cal.set(Calendar.MONTH, 0);
    cal.set(Calendar.DATE, 1);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
  }

  private static Calendar getZeroedCalendar() {
    return (Calendar) cal.clone();
  }

  @Override
  public Long exec(Tuple tuple) throws IOException {
    if (tuple == null || tuple.size() < 2) {
      throw new IOException("Not enough values");
    }
    Tuple timeTuple = (Tuple) tuple.get(0);
    if (tuple.size() - 1 > timeTuple.size()) {
      throw new IOException("Not enough tuple values to convert. Specified " + (tuple.size() - 1)
              + " calendar fields, but time tuple only has " + timeTuple.size());
    }
    Calendar cal = getZeroedCalendar();
    for (int i = 0; i < tuple.size() - 1; i++) {
      String key = (String) tuple.get(i + 1);
      Integer calKey = calMap.get(key);
      if (calKey == null) {
        throw new IOException("Unknown Calendar field: " + key);
      }
      cal.set(calKey, (Integer) timeTuple.get(i));
    }
    return cal.getTimeInMillis();
  }

}
