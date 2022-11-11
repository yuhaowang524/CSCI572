# name: Yuhao Wang
# student ID: 5779881695
# CSCI 572 HW1

import json
from bs4 import BeautifulSoup
import time
from random import randint
from selenium import webdriver
from html.parser import HTMLParser


class SearchEngine:
    @staticmethod
    def search(query, sleep=True):
        if sleep:  # Prevents loading too many pages too soon
            time.sleep(randint(10, 100))
        temp_url = '+'.join(query.split())  # for adding + between words for the query
        # using DuckDuckGo
        url = 'https://www.duckduckgo.com/html/?q=' + temp_url
        op = webdriver.ChromeOptions()
        op.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36")
        driver = webdriver.Chrome(executable_path="/Users/yuhaowang/Documents/GitHub/CSCI572/HW1/chromedriver",
                                  options=op)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        new_results = SearchEngine.scrape_search_result(soup)
        return new_results

    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all("a", attrs={"class": "result__a"})  # using DuckDuckGo
        ads_link = "https://duckduckgo.com/y.js?ad_domain"
        results = []
        # implement a check to get only 10 results and also check that URLs must not be duplicated
        for result in raw_results:
            link = result.get('href')
            if len(results) == 10:
                break
            if ads_link in link:
                continue
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


def task_one_helper(queries):
    duckduckgo_result = {}
    for count, query in enumerate(queries, 1):
        query = query.rstrip()
        duckduckgo_result[query] = SearchEngine.search(query)
        while len(duckduckgo_result[query]) < 10:
            print("Attention: query " + str(count) + " contains " +
                  str(len(duckduckgo_result[query])) + " search results.")
            print("retry searching query " + str(count))
            duckduckgo_result[query] = SearchEngine.search(query)
        else:
            print("query " + str(count) + " DuckDuckGo search complete...")
    return duckduckgo_result


def task_two_helper(queries, duckduckgo_result, google_result):
    stat_ret = {}
    for count, query in enumerate(queries, 1):
        query = query.rstrip()
        match_url = find_same_query(query, duckduckgo_result, google_result)
        overlapping_results = len(match_url)
        percent_overlap = (len(match_url) / 10) * 100
        rho = calculate_spearmans_rho(match_url)
        stat_ret[count] = [overlapping_results, percent_overlap, rho]
    return stat_ret


def result_printer(stat_results):
    average_overlapping_result, average_percent_overlap, average_spearman_coefficient = 0, 0, 0
    print("Queries, Number of Overlapping Results, Percent Overlap, Spearman Coefficient")
    for query in stat_results:
        print("Query " + str(query) + ", " + str(stat_results[query][0]) +
              ", " + str(stat_results[query][1]) + ", " + str(stat_results[query][2]))
        average_overlapping_result += stat_results[query][0] / 100
        average_percent_overlap += stat_results[query][1] / 100
        average_spearman_coefficient += stat_results[query][2] / 100
    print("Averages, " + str(average_overlapping_result) + ", " + str(average_percent_overlap) + ", " +
          str(average_spearman_coefficient))


def find_same_query(query, duckduckgo_result, google_result):
    """
    locate the same url link between google search result and duckduckgo search result
    :param query: each query fetched from txt file
    :param duckduckgo_result: url list fetched from DuckDuckGo Search engine
    :param google_result: url list fetched from Google result json file
    :return: matching url from two list
    """
    results = []
    for ggl_count, ggl_url in enumerate(google_result[query]):
        ggl_url = reduce_url_helper(ggl_url)
        for ddg_count, ddg_url in enumerate(duckduckgo_result[query]):
            ddg_url = reduce_url_helper(ddg_url)
            if ggl_url == ddg_url:
                results.append((ggl_count, ddg_count))
    return results


def reduce_url_helper(url):
    """
    set metrics for similar urls
    :param url:
    :return:
    """
    # Ignore "https://" and "http://" in the URL
    index = url.find("//")
    if index > -1:
        url = url[index + 2:]
    # Ignore "www." in the URL
    if "www." in url:
        url = url[4:]
    # Ignore "/" at the end of the URL
    if url[-1] == "/":
        url = url[:-1]
    return url.lower()


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


def write_result(file_name, duckduckgo_results):
    with open(file_name, "w") as f:
        json.dump(duckduckgo_results, f, indent=4)


def read_json_result(file_name):
    with open(file_name, "r") as j:
        results = json.load(j)
    return results


def check_result(queries, duckduckgo_result):
    for count, query in enumerate(queries, 1):
        query = query.rstrip()
        if len(duckduckgo_result[query]) != 10:
            print("Attention: query " + str(count) + " contains " +
                  str(len(duckduckgo_result[query])) + " search result.")


def write_csv(file_name, stat_results):
    average_overlapping_result, average_percent_overlap, average_spearman_coefficient = 0, 0, 0
    with open(file_name, "w") as f:
        f.write("Queries, Number of Overlapping Results, Percent Overlap, Spearman Coefficient\n")
        for query in stat_results:
            f.write(f"Query {query}, {stat_results[query][0]}, {stat_results[query][1]}, {stat_results[query][2]}\n")
            average_overlapping_result += stat_results[query][0] / 100
            average_percent_overlap += stat_results[query][1] / 100
            average_spearman_coefficient += stat_results[query][2] / 100
        f.write(f"Average, {average_overlapping_result}, {average_percent_overlap}, {average_spearman_coefficient}")


def json_combine_helper(file_one, file_two, output_file_name):
    with open(file_one, "r") as f1:
        main_file = json.load(f1)
    with open(file_two, "r") as f2:
        side_file = json.load(f2)
    for side_key in side_file:
        main_file[side_key] = side_file[side_key]
    with open(output_file_name, "w") as f:
        json.dump(main_file, f, indent=4)


def main():
    queries = read_queries()
    google_result = read_google_result()
    duckduckgo_result = read_json_result("data/hw1.json")
    stat_result = task_two_helper(queries, duckduckgo_result, google_result)
    write_csv("data/hw1.csv", stat_result)


if __name__ == "__main__":
    main()
