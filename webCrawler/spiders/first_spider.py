import scrapy
import re
import MySQLdb
import tldextract as tld
import urlparse

class FirstSpider(scrapy.Spider):
    name = "first"
    start_urls = [
        "http://www.reddit.com/",
        "http://www.bbc.com/",
        "http://www.huffingtonpost.com/",
        "https://en.wikipedia.org/wiki/List_of_state_leaders_in_755",
        "https://en.wikipedia.org/wiki/Whiting,_Wisconsin",
        "https://en.wikipedia.org/wiki/T._S._Raghavendra"
    ]

    def parse(self, response):
        scrapy.log.msg("#### CURRENT QUEUE: %d" % self.crawler.stats.get_value('scheduler/enqueued',0))
        db = MySQLdb.connect(host="localhost",user="root", passwd="", db="thesis")
        cur = db.cursor()
        #scrapy.log.msg("URL: %s", response.url)
        if re.search('index\.(php|html)$', response.url) != None or re.search('/$', response.url) != None :
            cleanUrl = re.sub('index\.(php|html)$', '', response.url)
            ex = tld.extract(cleanUrl)
            parsedURL = urlparse.urlsplit(cleanUrl)
            cleanUrl = "%s://%s/"% (parsedURL.scheme, ex.registered_domain)
            #scrapy.log.msg("---visiting INDEX: %s" % cleanUrl)
            try:
                cur.execute("INSERT INTO Links(url, gathered, visited, validated) VALUES('%s', CURRENT_TIMESTAMP, 1, 0)"% cleanUrl)
                #cur.execute("INSERT INTO pages(link, content) VALUES(%s, %s)", (cleanUrl, response.body))
                db.commit()
                #scrapy.log.msg("ADDED URL")
            except Exception, e:
                #scrapy.log.msg("ERROR: ")
                #scrapy.log.msg(str(e))
                pass
        else:
            scrapy.log.msg("visiting a regular link: %s" % response.url)
        if db:
            db.close()

        if self.crawler.stats.get_value('scheduler/enqueued',0) < 700000:
            for url in response.xpath('//a/@href').extract():

                absolute_url = urlparse.urljoin(response.url, url.strip())
                parsed_uri = urlparse.urlparse(absolute_url)
                e = tld.extract(url)
                domain = '%s://%s/' % (parsed_uri.scheme, e.registered_domain)
                yield scrapy.Request(domain, callback=self.parse)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
