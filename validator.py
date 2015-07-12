import sys
import MySQLdb
import json
import requests
import bs4
import urllib2
import socket
socket.setdefaulttimeout(15)

def doctype(soup):
    items = [item for item in soup.contents if isinstance(item, bs4.Doctype)]
    return items[0] if items else None

def metaContent(soup):
    retval = []
    metaTags = soup.findAll("meta")
    for meta in metaTags:
        retval.append(meta.attrs)
    return retval


db = MySQLdb.connect(host="localhost",user="root", passwd="", db="thesis", charset='utf8', init_command='SET NAMES UTF8')
cur = db.cursor()
counter = 0
successes = 0
sortOrder = sys.argv[1]
offset = sys.argv[2]
while True:
    cur.execute("SELECT linkId, url FROM Links WHERE validated = FALSE ORDER BY url " + sortOrder + " LIMIT 20 OFFSET " + offset)
    rows = cur.fetchall()
    for row in rows:
        counter += 1
        link = row[1]
        r = requests.get("http://localhost/w3c-validator/check?uri=%s&output=json" % link)
        """try:
            print "url: ", row[1]
            response = urllib2.urlopen(row[1])
            html = response.read().decode("utf-8", "ignore")
            soup = bs4.BeautifulSoup(html)
            pTitle = soup.title.decode("utf-8", "ignore")
            pDoctype = doctype(soup).decode("utf-8", "ignore")
            pMeta = metaContent(soup)
            pMeta = json.dumps(pMeta).decode("utf-8", "ignore")
            cur.execute("INSERT INTO Pages (link, title, doctype, meta) VALUES(%s, %s, %s, %s)", (row[0], pTitle, pDoctype, pMeta))
        except Exception, e:
            print "Error with fetching pages"
            print e
        """
        try:
            jsonResult = json.loads(r.content.decode('utf-8'))
            for message in jsonResult['messages']:
                mContent = message['message']# if 'message' in message else ''
                explanation = message['explanation'] if 'explanation' in message else ''
                mType = message['type'] if 'type' in message else ''
                subtype = message['subtype'] if 'subtype' in message else ''
                messageid = message['messageid'] if 'messageid' in message else ''
                cur.execute("INSERT INTO Validation_Errors (link, linkingId, message, explanation, type, subtype, message_id) VALUES(%s, %s, %s, %s, %s, %s, %s)", (link, row[0], mContent, explanation, mType, subtype, messageid))
            successes += 1
        except Exception, e:
            cur.execute("INSERT INTO Validation_Errors (link, linkingId, message, explanation, type, subtype, message_id) VALUES(%s, %s, 'Error', %s, '', '', '')", (link, row[0], e))
        cur.execute("""UPDATE Links SET validated = TRUE WHERE linkId = %s LIMIT 1""", (row[0],))
    db.commit()
    print successes, "/", counter, " | success/total (", int(((successes/counter)*100)), "%)"
