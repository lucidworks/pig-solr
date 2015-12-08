package com.lucidworks.hadoop.pig;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import com.lucidworks.hadoop.utils.MockMapReduceOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

/**
 *
 *
 **/
public class SolrStoreFuncTest {

  @Test
  public void testLocation() throws Exception {
    SolrStoreFunc func = new SolrStoreFunc();
    Job job = new Job();
    func.setStoreLocation("zk://host:1888", job);
    Assert.assertEquals("host:1888", job.getConfiguration().get("solr.zkhost"));
    //solr server url
    job = new Job();
    func.setStoreLocation("http://host:1888/solr", job);
    Assert.assertEquals("http://host:1888/solr", job.getConfiguration().get("solr.server.url"));
    job = new Job();
    func.setStoreLocation("host:1888", job);
    Assert.assertEquals("host:1888", job.getConfiguration().get("solr.zkhost"));

  }

  @Test
  public void testBadTuple() throws Exception {
    final MockMapReduceOutputFormat mock = new MockMapReduceOutputFormat();
    SolrStoreFunc func = new SolrStoreFunc() {
      //mock out the output format, as we have separate tests for testing those
      @Override
      public OutputFormat getOutputFormat() throws IOException {
        return mock;
      }
    };
    func.prepareToWrite(mock.getRecordWriter(null));
    Tuple tuple = new DefaultTuple();
    tuple.append("id-1");
    try {
      func.putNext(tuple);
      fail();
    } catch (IOException e) {
      //expected
    }
    try {
      func.putNext(null);
      fail();
    } catch (IOException e) {
      //expected
    }

    try {
      func.putNext(createTuple("id", "odd", "even", "odd2", "even2", "odd3"/*no even*/));
      fail();
    } catch (IOException e) {
      //expected
    }

  }

  @Test
  public void testStore() throws Exception {
    final MockMapReduceOutputFormat mock = new MockMapReduceOutputFormat();
    SolrStoreFunc func = new SolrStoreFunc() {
      //mock out the output format, as we have separate tests for testing those
      @Override
      public OutputFormat getOutputFormat() throws IOException {
        return mock;
      }
    };
    func.prepareToWrite(mock.getRecordWriter(null));
    Tuple tuple = new DefaultTuple();
    tuple.append("id-1");
    for (int i = 0; i < 10; i++) {
      tuple.append("key_" + i);
      tuple.append("value_" + i);
    }
    //put on some nested tuples to simulate multivalued fields
    Tuple nest = createTuple("1", "2", "3", "4");
    tuple.append("nest");
    tuple.append(nest);
    tuple.append("nest2");
    //all recursive tuples get rolled up as multivalued fields onto the field
    Tuple child = createTuple("a", "b", "c", createTuple("aa", "bb", "cc"));
    tuple.append(child);
    tuple.append("nest3");
    child = createTuple("a", "b", "c", createTuple("aa", "bb", "cc"), createTuple("aaa", "bbb", "ccc"));
    tuple.append(child);
    tuple.append("databag");
    DataBag databag = new DefaultDataBag();
    databag.add(createTuple("f", "g"));
    databag.add(createTuple("h", "i"));
    tuple.append(databag);
    func.putNext(tuple);
    Assert.assertEquals(1, mock.writer.map.size());
    LWDocumentWritable writable = mock.writer.map.get("id-1");
    LWDocument doc = writable.getLWDocument();
    Assert.assertNotNull("doc", doc);
    Object nestFields = doc.getFirstFieldValue("nest");
    Assert.assertNotNull("nestFields", nestFields);
    // TODO: Tests nest fields.

  }

  protected Tuple createTuple(Object... entry) {
    Tuple result = new DefaultTuple();
    for (Object o : entry) {
      result.append(o);
    }
    return result;
  }
}
