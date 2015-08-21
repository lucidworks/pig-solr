package com.lucidworks.hadoop.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

//import org.apache.pig.Algebraic;

public class Histogram extends EvalFunc<Tuple> /* implements Algebraic */ {

  private static TupleFactory tupleFactory = TupleFactory.getInstance();
  private static Logger log = LoggerFactory.getLogger(Histogram.class);

  @Override
  public Tuple exec(Tuple tuple) throws IOException {
    if (tuple == null || tuple.size() < 4) {
      throw new IOException("Invalid input, expecting four values: data bag, min value, max value, number of bins");
    }
    DataBag bag = (DataBag) tuple.get(0);
    double min = ((Number) tuple.get(1)).doubleValue(); // Min value
    double max = ((Number) tuple.get(2)).doubleValue(); // Max value
    int nbins = (Integer) tuple.get(3); // Number of bins
    return exec(bag, min, max, nbins);
  }

  /*
   * Convenience method with real arguments instead of a Tuple
   */
  public static Tuple exec(DataBag bag, double min, double max, int nbins) throws IOException {
    int width = Histogram.calculateWidth(min, max, nbins);
    log.info("Histogram properties: min={}, max={}, nbins={}, width={}", new Object[] { min, max, nbins, width });
    return binData(bag, nbins, width, min);
  }

  /*
   * Determine the bin width give a min, max, and the number of requested bins. Minimum is one bin.
   * Throws IOExecption if nbins is less than zero
   */
  protected static int calculateWidth(double min, double max, int nbins) throws IOException {
    if (nbins <= 0) {
      throw new IOException("Must specify a positive number of bins. " + nbins + " is leq zero");
    }
    return (int) Math.ceil((max - min + 1) / nbins);
  }

  /*
   * Bin the data. Values are shifted by the min so the histogram only covers the range of the data
   * being binned. One unsafe thing here is that min is not checked to actually be less than each 
   * value as it iterates. This is done to eliminate unnecessary branching (and lots of missed 
   * branch prediction) within the loop.
   */
  protected static Tuple binData(DataBag values, int nbins, int width, double min) throws ExecException {
    try {
      System.err.println(values + " " + nbins + " " + width + " " + min);
      long[] bins = new long[nbins];
      Iterator<Tuple> it = values.iterator();
      double val = 0;
      int bin = 0;
      while (it.hasNext()) {
        Tuple next = it.next();
        val = ((Number) next.get(0)).doubleValue() - min; // Shift by the min
        bin = (int) Math.floor(val / width);
        bins[bin]++;
      }
      Tuple tuple = tupleFactory.newTuple(bins.length);
      for (int i = 0; i < bins.length; i++) {
        tuple.set(i, bins[i]);
      }
      return tuple;
    } catch (Exception e) {
      log.warn("Exception while processing bins={}, width={}, values={}", new Object[] { nbins, width, values });
      throw new ExecException(e.getCause());
    }
  }

  /*
   * Combine two histograms together. Assumed that all the histograms are the same size.
   * TODO where is this used?
   */
  /*protected static Tuple combine(DataBag bag) throws ExecException {
    Iterator<Tuple> it = bag.iterator();
    Tuple bins = it.next();
    while (it.hasNext()) {
      Tuple next = it.next();
      for (int i = 0; i < bins.size(); i++) {
        bins.set(i, (Long) bins.get(i) + (Long) next.get(i));
      }
    }
    return bins;
  }*/

  /*
   * Algebraic interface TODO fix this eventually?
   */
  /*
  public static class Initial extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
      if(tuple == null || tuple.size() < 4) {
        throw new IOException("Invalid input, expecting four values: data bag, min value, max value, number of bins");
      }
      double min = ((Number)tuple.get(1)).doubleValue(); // Min value
      double max = ((Number)tuple.get(2)).doubleValue(); // Max value
      if(max <= min) {
        throw new IOException("Invalid min/max. Expected min < max.");
      }
      int nbins = (Integer)tuple.get(3); // Number of bins
      if(nbins == 0) {
        throw new IOException("Must specify a positive number of bins");
      }
      int width = (int) Math.ceil((max-min) / nbins);
      return binData((DataBag)tuple.get(0), nbins, width);      
    }
    
  }
  
  public static class Intermediate extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
      return combine((DataBag)tuple.get(0));
    }
    
  }
  
  public static class Final extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
      return combine((DataBag)tuple.get(0));
    }
    
  }
  
  @Override
  public String getFinal() {
    return Final.class.getName();
  }

  @Override
  public String getInitial() {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed() {
    return Intermediate.class.getName();
  }
  */
}
