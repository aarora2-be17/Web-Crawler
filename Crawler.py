import sqlite3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen

class Crawler:
    def CreateDatabase(self):
        db = sqlite3.connect('spider5.sqlite')
        cur = db.cursor()
        
        cur.execute('''CREATE TABLE IF NOT EXISTS Pages
            (id INTEGER PRIMARY KEY, url TEXT UNIQUE,
             error INTEGER, page_rank REAL)''')
        
        cur.execute('''CREATE TABLE IF NOT EXISTS Links
            (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))''')
        
        cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')
        return (db ,cur)
    
    def getURL(self):
        #To check if DATABASE is empty
        (db, cur) = self.CreateDatabase()
        cur.execute('SELECT id,url FROM Pages WHERE error is NULL ORDER BY RANDOM() LIMIT 1')
        row = cur.fetchone()
        
        #If database is not empty - Restart the previous process
        if row is not None:
            print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")

        else :
            starturl = input('Enter web url or enter: ')
            if ( len(starturl) < 1 ) : starturl = 'https://en.wikipedia.org/wiki/WWE'
            if ( starturl.endswith('/') ) : starturl = starturl[:-1]
            web = starturl
            if ( starturl.endswith('.htm') or starturl.endswith('.html') ) :
                pos = starturl.rfind('/')
                web = starturl[:pos]
        
            if ( len(web) > 1 ) :
                cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', ( web, ) )
                cur.execute('INSERT OR IGNORE INTO Pages (url, page_rank) VALUES ( ?, 1.0 )', ( web, ) )
                db.commit()
    
    def getCurrentWebsites(self):
        
        (db, cur) = self.CreateDatabase()
        cur.execute('''SELECT url FROM Webs''')
        webs = list()
        for row in cur:
            webs.append(str(row[0]))

        return webs
    
    def startCrawling(self):
        (conn, cur) = self.CreateDatabase()
        webs = self.getCurrentWebsites()
        many = 0
        while True:
            if ( many < 1 ) :
                sval = input('How many pages:')
                if ( len(sval) < 1 ) : break
                many = int(sval)
            many = many - 1
        
            cur.execute('SELECT id,url FROM Pages WHERE error is NULL ORDER BY RANDOM() LIMIT 1')
            try:
                row = cur.fetchone()
                # print row
                fromid = row[0]
                url = row[1]
            except:
                print('No unretrieved HTML pages found')
                many = 0
                break
        
            print(fromid, url)
            
            html_page = urlopen(url)
            soup = BeautifulSoup(html_page, "lxml")
        
            for link in soup('a'):
                href = link.get('href')
                # Resolve relative references like href="/contact"
                up = urlparse(href)
                if ( len(up.scheme) < 1 ) :
                    href = urljoin(url, href)
                    
                ipos = href.find('#')
                if ( ipos > 1 ) : href = href[:ipos]
                
                if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) : continue
                if ( href.endswith('/') ) : href = href[:-1]
                
                # print href
                if ( len(href) < 1 ) : continue
                
                Found = False
                for i in webs:
                    if href.startswith(i):
                        Found = True
                        break
                
                if not Found:
                    continue
                
                cur.execute('INSERT OR IGNORE INTO Pages (url, page_rank) VALUES ( ?, 1.0 )', ( href, ) )
                conn.commit()
        
                cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
                try:
                    row = cur.fetchone()
                    toid = row[0]
                except:
                    print('Could not retrieve id')
                    continue
                # insert fromid, toid
                cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( fromid, toid ) )
        cur.close()
                    

spider = Crawler()

(x,y) = spider.CreateDatabase()
spider.getURL()
print(spider.getCurrentWebsites())
spider.startCrawling()