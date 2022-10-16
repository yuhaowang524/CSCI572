import http
import pandas as pd


def main():
    with open("fetch_latimes.csv", "r", encoding="UTF-8") as f:
        data = pd.read_csv(f, header=0)
        fetches_attempted = data.shape[0]
        fetches_succeeded = data[data["Status"] < 300].shape[0]
        fetches_failed = data[data["Status"] > 300].shape[0]
        status_codes = data.groupby(data["Status"]).count().to_dict()["URL"]

    with open("visit_latimes.csv", "r", encoding="UTF-8") as f:
        data = pd.read_csv(f, header=0)
        total_urls_extracted = data["Outgoing Links"].sum()
        less_1KB = data[data["Size"] < 1024].shape[0]
        less_10KB = data[(data["Size"] < 10 * 1024) & (data["Size"] >= 1024)].shape[0]
        less_100KB = data[(data["Size"] < 100 * 1024) & (data["Size"] >= 10 * 1024)].shape[0]
        less_1MB = data[(data["Size"] < 1024 * 1024) & (data["Size"] >= 100 * 1024)].shape[0]
        greater_1MB = data[data["Size"] >= 1024 * 1024].shape[0]
        content_types = data.groupby(data["Content Type"]).count().to_dict()["URL"]

    with open("urls_latimes.csv", "r", encoding="UTF-8") as f:
        data = pd.read_csv(f, header=0)
        unique_extracted = data.shape[0]
        unique_within = data[data["Status"] == "OK"].shape[0]
        unique_outside = data[data["Status"] == "N_OK"].shape[0]

    with open("CrawlReport_latimes.txt", "w") as f:
        f.write(f"Name: Yuhao Wang\n")
        f.write(f"USC ID: 5779881695\n")
        f.write(f"News site crawled: latimes.com\n")
        f.write(f"Number of threads: 7\n")
        f.write(f"\n")

        f.write(f"Fetch Statistics\n")
        f.write(f"================\n")
        f.write(f"fetches attempted: {fetches_attempted}\n")
        f.write(f"fetches succeeded: {fetches_succeeded}\n")
        f.write(f"fetches failed or aborted: {fetches_failed}\n")
        f.write(f"\n")

        f.write(f"Outgoing URLs:\n")
        f.write(f"==============\n")
        f.write(f"Total URLs extracted: {total_urls_extracted}\n")
        f.write(f"# unique URLs extracted: {unique_extracted}\n")
        f.write(f"# unique URLs within News Site: {unique_within}\n")
        f.write(f"# unique URLs outside News Site: {unique_outside}\n")
        f.write(f"\n")

        f.write(f"Status Codes:\n")
        f.write(f"=============\n")
        for code in sorted(status_codes.keys()):
            f.write(f"{code} {http.HTTPStatus(code).phrase}: {status_codes[code]}\n")
        f.write(f"\n")

        f.write(f"File Sizes:\n")
        f.write(f"===========\n")
        f.write(f"< 1KB: {less_1KB}\n")
        f.write(f"1KB ~ <10KB: {less_10KB}\n")
        f.write(f"10KB ~ <100KB: {less_100KB}\n")
        f.write(f"100KB ~ <1MB: {less_1MB}\n")
        f.write(f">= 1MB: {greater_1MB}\n")
        f.write(f"\n")

        f.write(f"Content Types:\n")
        f.write(f"==============\n")
        for content in sorted(content_types.keys()):
            f.write(f"{content}: {content_types[content]}\n")


if __name__ == "__main__":
    main()
