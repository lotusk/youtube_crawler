import scrapy
import json
import os
import urllib


class YBSearchSpider(scrapy.Spider):
    name = "quotes"
    try:
        key = os.environ['YOUTUBE_KEY']  # AIzaSyDu9IzmKRZsiFq2tPVb2qFLxqeGBSa6GMc
    except:
        print("Please make sure YOUTUBE_KEY in Env")
        raise

    url_template = "https://www.googleapis.com/youtube/v3/search?part=snippet&videoCategoryId=27&type=video&"
    max_result = 50
    # download_delay = 10000/3600.0  # Throttle  set in settings.py

    keywords = [
        "hadoop",
        "spark",
        "airflow",
        "tensorflow",
        "keras",
        "scala",
        "python",
        "java",
        "storm",
        "lstm",
        "nlp"
    ]

    def start_requests(self):
        urls = [
            self.url_template + urllib.parse.urlencode({"q": keyword, "maxResults": self.max_result, "key": self.key})
            for keyword in
            self.keywords]
        # urls = urls[:1]  # I think scrapy will have a config about max request , find it later
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        for item in jsonresponse.get("items", []):
            yield item
