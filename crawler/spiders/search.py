import scrapy
import json


class YBSearchSpider(scrapy.Spider):
    name = "quotes"
    key = "AIzaSyDu9IzmKRZsiFq2tPVb2qFLxqeGBSa6GMc"
    url_template = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=%s&maxResults=%s&videoCategoryId=27&type=video&key=" + key
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
        urls = [self.url_template % (keyword, self.max_result) for keyword in self.keywords]
        # urls = urls[:1000]  # I think scrapy will have a config about max request , find it later
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        for item in jsonresponse.get("items", []):
            yield item
