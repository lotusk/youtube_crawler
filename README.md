# youtube_crawler
---

### dependences
* scrapy 1.5.1
* airflow 1.10.1
* python 3.5.2
* elasticsearch 6.5.3
* pyspark (spark 2.4.0)

### elastic mapping init

<pre>
curl -X PUT "localhost:9200/ybvideos" -H 'Content-Type: application/json' -d'
{
    "settings" : {
        "number_of_shards" : 1
    },
    "mappings" : {
        "_doc" : {
            "properties" : {
            	"snippet":{
	            	"properties": {
	            		"channelId" : { "type" : "keyword" },
		                "title" : {"type" : "text" ,"analyzer": "standard"},
		                "description" : {"type" : "text" ,"analyzer": "standard"},
		                "channelTitle": {"type" : "text" ,"analyzer": "standard"},
		                "publishedAt": {"type" : "date"}
	            	}
            	},
            	"id":{
            		"properties" : {
            			"videoId":{ "type" : "keyword" }
            		}
            	},
         		"etag":{"type" : "keyword"} 
            }
        }
    }
}
'
</pre>

### run scrapy
* `scrapy runspider crawler/spiders/search.py -o ybi.jl`

### run index
* `python indexs/writer.py --file ybi.jl` 

### run counter analyzer
* `spark-submit analyzers/count.py --file ybi.jl --option word`
* `spark-submit analyzers/count.py --file ybi.jl --option channeltitle`

###
<pre>
cp dags/pipeline.py ~/airflow/dags/
python ~/airflow/dags/pipeline.py

airflow test youtube_crawler scrapy 2018-12-15
airflow test youtube_crawler index 2018-12-15
airflow test youtube_crawler word_count 2018-12-15
airflow test youtube_crawler channel_title_count 2018-12-15

airflow backfill youtube_crawler -s 2018-12-15 
</pre>



## To Do List for Future Improvement
* I think just use download_delay is not good enough for throttle,we can set delay less than 10000/3600 (assume 1 hour 10000 is limit) and every hour give crawler 10000 token,we should stop when token use out.
* maybe keyword as a event triggle pipeline.
* check output file when report (for now  just print ok)
* transform youtube json format.
* filter datas invalid
* make a classifier using searchword,title ,catetory , channel and description .
* I don't know what is etag in response data. maybe useful.
* download the video.
* security.
