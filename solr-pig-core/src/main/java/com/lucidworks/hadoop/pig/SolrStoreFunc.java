package com.lucidworks.hadoop.pig;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentProvider;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import com.lucidworks.hadoop.io.LWMapReduceOutputFormat;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class SolrStoreFunc extends StoreFunc {
  private RecordWriter writer;
  private JobConf conf;

  public enum WARNINGS {
    NULL_ID
  }

  public SolrStoreFunc() {

  }

  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new LWMapReduceOutputFormat();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
  }

  @Override
  public void putNext(Tuple tuple) throws IOException {
    // first entry is doc id
    // remaining entries are (key, value) pairs
    //size must be odd
    if (tuple == null || tuple.size() < 3 || (tuple.size() % 2 == 0)) {
      throw new IOException("Incorrect number of values.");
    }
    try {
      Object id = tuple.get(0);
      if (id != null) {
        LWDocument solrDoc = LWDocumentProvider.createDocument();
        solrDoc.addField("id", id.toString());
        for (int i = 1; i < tuple.size(); i += 2) {
          String key = (String) tuple.get(i);
          Object value = tuple.get(i + 1);
          addValue(solrDoc, key, value);
        }
        writer.write(new Text(id.toString()), new LWDocumentWritable(solrDoc));
      } else {
        warn("Skipping Tuple " + tuple + " with a null id", WARNINGS.NULL_ID);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  protected void addValue(LWDocument solrDoc, String key, Object value) {
    if (value instanceof Tuple) {
      addTuple(solrDoc, key, (Tuple) value);
    } else if (value instanceof DataBag) {
      for (Tuple tup : (DataBag) value) {
        addTuple(solrDoc, key, tup);
      }
    } else {
      solrDoc.addField(key, value);
    }
  }

  private void addTuple(LWDocument solrDoc, String key, Tuple value) {
    List<Object> all = ((Tuple) value).getAll();
    for (Object val : all) {
      addValue(solrDoc, key, val);
    }
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    String zkHost = job.getConfiguration().get("solr.zkhost");
    if (zkHost == null) {
      if (location.startsWith("zk://")) {
        location = location.substring("zk://".length());
        job.getConfiguration().set("solr.zkhost", location);
      } else if (location.startsWith("http:")) {
        job.getConfiguration().set("solr.server.url", location);
      } else {
        //assume zk
        job.getConfiguration().set("solr.zkhost", location);
      }
    }
    conf = (JobConf) job.getConfiguration();
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
    // do nothing
  }

}