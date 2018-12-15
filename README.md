# youtube_crawler
---
###dependences
* scipy
* airflow
* python
* elasticsearch
* pyspark

##elastic mapping init
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


