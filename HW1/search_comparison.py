from bs4 import BeautifulSoup
import time
import requests
from random import randint
from html.parser import HTMLParser

USER_AGENT = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                           '(KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

class SearchEngine:
    @staticmethod
    def search(query, sleep=True):
        if sleep: # Prevents loading too many pages too soon
            time.sleep(randint(10, 100))
        temp_url = '+'.join(query.split()) #for adding + between words for the query
        url = 'SEARCHING_URL' + temp_url
        soup = BeautifulSoup(requests.get(url, headers=USER_AGENT).text, "html.parser")
        new_results = SearchEngine.scrape_search_result(soup)
        return new_results

    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all("SEARCH SELECTOR")
        results = []
        #implement a check to get only 10 results and also check that URLs must not be duplicated
        for result in raw_results:
            link = result.find('a').get('href')
            results.append(link)
        return results


def read_queries():
    with open("data/100QueriesSet4.txt", "r") as f:
        queries = f.readlines()
    return queries



def main():


if __name__ == "__main__":
    main()