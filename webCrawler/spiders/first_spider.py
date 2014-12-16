import scrapy
import re
import MySQLdb
import urlparse

class FirstSpider(scrapy.Spider):
    name = "first"
    start_urls = [
        "http://www.reddit.com/",
        "http://www.bbc.com/",
        "http://www.huffingtonpost.com/",
        "https://en.wikipedia.org/wiki/Special:Random"
    ]
    def parse(self, response):
        scrapy.log.msg("#### CURRENT QUEUE: %d" % self.crawler.stats.get_value('scheduler/enqueued',0))
        db = MySQLdb.connect(host="localhost",user="root", passwd="", db="crawler")
        cur = db.cursor()
        if re.search('index\.(php|html)$', response.url) != None or re.search('/$', response.url) != None :
            cleanUrl = re.sub('index\.(php|html)$', '', response.url)
            scrapy.log.msg("---visiting INDEX: %s" % cleanUrl)
            try:
                cur.execute("INSERT INTO links(link, visited, validated) VALUES(%s, 1, 0)", cleanUrl)
                cur.execute("INSERT INTO pages(link, content) VALUES(%s, %s)", (cleanUrl, response.body))
                db.commit()
            except:
                pass
        else:
            scrapy.log.msg("visiting a regular link: %s" % response.url)
        if db:
            db.close()

        if self.crawler.stats.get_value('scheduler/enqueued',0) < 700000:
            for url in response.xpath('//a/@href').extract():
                absolute_url = urlparse.urljoin(response.url, url.strip())
                parsed_uri = urlparse.urlparse(absolute_url)
                domain = '%s://%s/' % (parsed_uri.scheme, parsed_uri.netloc)
                yield scrapy.Request(domain, callback=self.parse)

