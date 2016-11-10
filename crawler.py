# _*_ coding: utf-8 _*_
import sqlite3 as sqlite
import requests
import chardet
from bs4 import BeautifulSoup


class MySqlite(object):
    """A simple disk-based database."""

    def __init__(self, db_file):
        try:
            self.connection = sqlite.connect(db_file)
            self.cursor = self.connection.cursor()
        except:
            print('Connection Failed!')
            raise

    def create_table(self, name):
        """Create a table to store crawled data."""
        statement = "CREATE TABLE IF NOT EXISTS {}(ID INTEGER PRIMARY KEY AUTOINCREMENT, " \
                    "Data VARCHAR(600))".format(name)
        try:
            self.cursor.execute(statement)
            self.connection.commit()
        except sqlite.Error:
            print('Table creation failed.')
            self.connection.rollback()

    def insert(self, name, data):
        """Insert crawled data into a database table."""
        statement = "INSERT INTO {}(Data) VALUES('{}')".format(name, data)
        print(statement)
        try:
            self.cursor.execute(statement)
            self.connection.commit()
        except sqlite.Error:
            print('Insertion Failed!')
            self.connection.rollback()

    def close(self):
        """Close the cursor and current connection."""
        self.cursor.close()
        self.connection.close()


class Spider(object):
    """A Web Spider class."""

    def __init__(self, db_file, table, start_url, key_word=None, depth=10):
        self.db = MySqlite(db_file)
        self.db.create_table(table)
        self.table = table
        self.start_url = start_url
        self.key_word = key_word
        self.depth = depth
        self.visited = []  # Store visited urls.
        # TO_DO: switch headers

    def crawl(self, url, key, depth):
        """Send requests to url."""
        if url in self.visited or depth < 0:
            return
        response = requests.get(url)
        if response.status_code != 200:
            print("Sending requet to {} failed.".format(url))
            return
        # Detect encoding of response.
        encoding = chardet.detect(response.content)
        response.encoding = encoding
        # Parse page.
        self.parse(response, key, depth)

    def parse(self, response, key, depth):
        """Parse page and store information to sqlite."""
        if depth < 1:
            return
        soup = BeautifulSoup(response.text, 'lxml')
        # Check if key words in the content.
        content = soup.get_text()
        if key in content:
            self.db.insert(self.table, content)
        # Extract links in response.
        links = [link.get('href') for link in soup.find_all('a')]
        for link in links:
            self.crawl(link, key, depth-1)

    def run(self):
        """A wrapper function to start crawling."""
        self.crawl(self.start_url, self.key_word, self.depth)
