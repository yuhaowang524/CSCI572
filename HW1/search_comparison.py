# name: Yuhao Wang
# student ID: 5779881695
# CSCI 572 HW1

import json
from bs4 import BeautifulSoup
import time
import requests
from random import randint
from html.parser import HTMLParser

USER_AGENT = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                            '(KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}


class SearchEngine:
    @staticmethod
    def search(query, sleep=True):
        if sleep:  # Prevents loading too many pages too soon
            time.sleep(randint(10, 100))
        temp_url = '+'.join(query.split())  # for adding + between words for the query
        # using DuckDuckGo
        url = 'https://www.duckduckgo.com/html/?q=' + temp_url
        soup = BeautifulSoup(requests.get(url, headers=USER_AGENT).text, "html.parser")
        new_results = SearchEngine.scrape_search_result(soup)
        return new_results

    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all("a", attrs={"class": "result__a"})  # using DuckDuckGo
        results = []
        # implement a check to get only 10 results and also check that URLs must not be duplicated
        for result in raw_results:
            link = result.get('href')
            if len(results) == 10:
                break
            if link not in results:
                results.append(link)
        return results


def read_queries():
    with open("data/100QueriesSet4.txt", "r") as f:
        queries = f.readlines()
    return queries


def read_google_result():
    with open("data/Google_Result4.json", "r") as j:
        result = json.load(j)
    return result


def find_same_query(query, duckduckgo_result, google_result):
    """
    :param query: each query fetched from txt file
    :param duckduckgo_result: url list fetched from DuckDuckGo Search engine
    :param google_result: url list fetched from Google result json file
    :return: matching url from two list
    """
    results = []
    for ddg_count, ddg_url in enumerate(duckduckgo_result[query]):
        ddg_url = reduce_url_helper(ddg_url)
        for ggl_count, ggl_url in enumerate(google_result[query]):
            ggl_url = reduce_url_helper(ggl_url)
            if ggl_url == ddg_url:
                results.append((ggl_count, ddg_count))
    return results


def reduce_url_helper(url):
    # Ignore "https://" and "http://" in the URL
    index = url.find("//")
    if index > -1:
        url = url[index + 2:]
    # Ignore "www." in the URL
    if "www." in url:
        url = url[4:]
    return url.lower()


def task_helper(queries, google_result):
    duckduckgo_result = {}

    for count, query in enumerate(queries, 1):
        query = query.rstrip()
        duckduckgo_result[query] = SearchEngine.search(query)
        print("query " + str(count) + " DuckDuckGo search complete...")
        match_url = find_same_query(query, duckduckgo_result, google_result)
        spearmans_rho = calculate_spearmans_rho(match_url)


    return duckduckgo_result


def calculate_spearmans_rho(match_url_list):
    if len(match_url_list) == 0:
        coefficient = 0
    elif len(match_url_list) == 1:
        if match_url_list[0][0] == match_url_list[0][1]:
            coefficient = 1
        else:
            coefficient = 0
    else:
        diff = 0
        for d1, d2 in match_url_list:
            diff += (d1 - d2) ** 2
        coefficient = 1 - ((6 * diff) / (len(match_url_list) * (len(match_url_list) ** 2 - 1)))
    return coefficient


def main():
    test_list = [[1, 1], [5, 9], [6, 2], [7, 6]]
    co = calculate_spearmans_rho(test_list)
    print(co)


if __name__ == "__main__":
    main()
