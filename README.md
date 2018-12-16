# youtube_crawler
---

### dependences
* scipy
* airflow
* python
* elasticsearch
* pyspark

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
* `scrapy runspider crawler/spiders/search.py -o ybl.jl`

### run index
* `python indexs/writer.py --file ybi.jl` 

### run wordcount analyzer
* `spark-submit analyzers/wordcount.py --file ybl.jl`

###
<pre>
cp dags/pipeline.py ~/airflow/dags/
python ~/airflow/dags/pipeline.py

airflow test youtube_crawler scrapy 2018-12-15
airflow test youtube_crawler index 2018-12-15
airflow test youtube_crawler wordcount 2018-12-15

airflow backfill youtube_crawler -s 2018-12-16  (does it a right way that backfill make all flow happen??)
</pre>


