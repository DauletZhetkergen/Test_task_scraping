import sqlite3
import time
import urllib
from datetime import datetime
from dateutil import parser
from bs4 import BeautifulSoup
import requests
import re
from lxml import etree
from urllib.request import urlopen, Request
import ssl
from lxml import html
import requests_html
conn = sqlite3.connect("database.db",check_same_thread=False)
cursor = conn.cursor()


cursor.execute("""CREATE TABLE IF NOT EXISTS resources(
    RESOURCE_ID integer PRIMARY KEY,
    RESOURCE_NAME varchar,
    RESOURCE_URL varchar,
    top_tag varchar,
    bottom_tag varchar,
    title_cut varchar,
    date_cut varchar
)""")
conn.commit()

cursor.execute("""CREATE TABLE IF NOT EXISTS items(
    id integer PRIMARY KEY,
    res_id integer,
    link varchar,
    title text,
    content text,
    nd_date integer,
    s_date integer,
    not_date date
)""")
conn.commit()
cursor.execute("SELECT RESOURCE_URL from resources ")
data_url = cursor.fetchall()


context = ssl._create_unverified_context()
htmlparser = etree.HTMLParser()
url = "https://kaztag.info/kz/news/vak-da-aza-standa-y-zha-a-o-u-zhylyna-dayyndy-boyynsha-bir-atar-tapsyrma-berildi-"
#
# source = html.fromstring(((requests.get(url)).text).encode('utf-8'))
# news_urls_old = source.xpath('//div[@class="content"]//div[@class="t-info"]/b/text()')
# dd = (" ".join(news_urls_old))
# date_new = parser.parse(dd[6:])
# print(date_new)
# print(''.join(news_urls_old))
# #######


context = ssl._create_unverified_context()
for i in data_url:

    cursor.execute("select RESOURCE_ID from resources where RESOURCE_URL=?", (i[0],))
    url_id = cursor.fetchone()
    cursor.execute("Select top_tag from resources where RESOURCE_URL=?", (i[0],))
    top_tag = cursor.fetchone()
    cursor.execute("Select bottom_tag from resources where resource_url=?", (i[0],))
    bottom_tag = cursor.fetchone()
    cursor.execute("Select title_cut from resources where resource_url=?", (i[0],))
    title_cut = cursor.fetchone()
    cursor.execute("Select date_cut from resources where resource_url=?", (i[0],))
    date_cut = cursor.fetchone()
    # htmlparser = etree.HTMLParser()
    # response = urlopen(i[0], context=context)
    # tree = etree.parse(response, htmlparser)
    source = html.fromstring(((requests.get(i[0])).text).encode('utf-8'))
    if i[0] == "https://kaztag.info/kz/":
        news_urls_old = source.xpath("{}".format(top_tag[0]))
        news_urls=[]
        for s in news_urls_old:
            news_urls.append("https://kaztag.info/kz/{}".format(s[4:]))
    else:
        news_urls = source.xpath("{}".format(top_tag[0]))

    for url_news in news_urls:
        # response = urlopen(url_news, context=context)
        # tree = etree.parse(response, htmlparser)
        # bottom_tag_data = tree.xpath("{}".format(bottom_tag[0]))
        source = html.fromstring(((requests.get(url_news)).text).encode('utf-8'))
        bottom_tag_data = source.xpath("{}".format(bottom_tag[0]))
        bottom_data_new =""
        for s in bottom_tag_data:
            if '\n' in s:
                s.strip()
            bottom_data_new += s
        title_cut_data = source.xpath("{}".format(title_cut[0]))

        title_data_new = ""

        for j in title_cut_data:
            if '\n' in j:
                re.sub("^\s+|\n|\r|\s+$", '', j)
            title_data_new +=j
        date_cut_data = source.xpath("{}".format(date_cut[0]))
        print(title_data_new)
        date_new = None
        date_new_str = None
        nd = None
        if date_cut_data[0][0]=="К":
            dd = (" ".join(date_cut_data))
            date_cut_data = parser.parse(dd[6:])
            date_new = date_cut_data
            date_new_str = str(date_cut_data)
        else:
            pass
            date_new = parser.parse(date_cut_data[0])
            date_new_str = str(parser.parse(date_cut_data[0]))
        nd = int(time.mktime(date_new.timetuple()))
        s_date = int(time.mktime(datetime.today().timetuple()))
        cursor.execute("INSERT INTO items (res_id,link,title,content,nd_date,s_date,not_date) values (?,?,?,?,?,?,?)",(url_id[0],url_news,title_data_new,bottom_data_new,nd,s_date,date_new_str))
        conn.commit()

