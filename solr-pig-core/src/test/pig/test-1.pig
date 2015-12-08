REGISTER 'hdfs:///hadoop/hadoop-lws-job.jar';

set solr.collection 'test-pig';

--A = load '/users/grantingersoll/gen-data.dat/pigmix_page_views/' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
A = load '$input/pigmix_page_views/' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);

-- B = foreach A generate user, action, (int)timespent as timespent, query_term, ip_addr, timestamp;
B = FOREACH A GENERATE $0, 'action_s', $1, 'timespent_s', $2, 'query_term_s', $3, 'ip_addr_s', $4, 'timestamp_s', $5, 'estimated_revenue_s', $6, 'page_info_s', $7, 'page_links_s', $8;

ok = store B into 'http://localhost:8888/solr' using com.lucidworks.hadoop.pig.SolrStoreFunc();

