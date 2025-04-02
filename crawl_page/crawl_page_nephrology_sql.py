import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import mysql.connector
import csv
from mysql.connector import Error
import re
import configparser
from utils import write_to_csv, insert_into_database , setup_database, fetch_page_with_zenrows, fetch_page_with_scraper_api

specialization = 'Nephrology'


def fetch_jasn_articles(conn):
    name ='Journal of the American Society of Nephrology'
    base_url = "https://journals.lww.com/jasn/toc"
    start_year = 2024
    end_year = 2021
    unique_links = set()  

    

    for year in range(start_year, end_year - 1, -1):  # Loop from 2024 to 2021
        for issue in range(1, 13):  # Loop over all 12 issues for each year
            issue_str = f"{issue:02}000"  # Format issue with leading zeros (e.g., 01000)
            issue_url = f"{base_url}/{year}/{issue_str}"
            print(f"Fetching: {issue_url}")
            try:
                # Fetch page using a helper function (e.g., ZenRows client)
                response = fetch_page_with_zenrows(issue_url)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "lxml")

                    
                    sections = soup.find_all("section", id="wp-articles-navigator", class_="content-box")

                    for section in sections:
                        header = section.find("header")
                        if header:
                            h3_tag = header.find("h3")
                            if h3_tag and "RESEARCH" in h3_tag.get_text(strip=True).upper():                             
                                h4_tags = section.find_all("h4")
                                for h4_tag in h4_tags:
                                    link = h4_tag.find("a", href=True)  # Find <a> tags in <h4>
                                    if link:
                                        href = urljoin(issue_url, link["href"])  # Resolve relative URLs
                                        if href not in unique_links:  # Deduplicate
                                            unique_links.add(href)
                                            print(f"Found: {href}")
                        else:
                            continue
                else:
                    print(f"Failed to fetch {issue_url}: Status Code {response.status_code}")
            except Exception as e:
                print(f"Error occurred while processing {issue_url}: {e}")

    # Convert set to list to maintain ordering
    article_links = list(unique_links)
    # Convert article links to a list
    article_links_list = list(article_links)
    # Insert into database
    insert_into_database(conn, name, article_links_list,specialization)

    print(f"Total unique articles found: {len(article_links)}")
    

def fetch_kidney_international_articles(conn):
    name = "Kidney International"
    base_url = "https://www.kidney-international.org"
    main_url = f"{base_url}/issues"
    group_ids = [f"d2020.v{num}" for num in range(106, 98, -1)] 
    unique_links = set()  
    for group_id in group_ids:
        issue_url = f"{main_url}?publicationCode=kint&issueGroupId={group_id}"
        print(f"Fetching : {issue_url}")
        try:
            response = fetch_page_with_zenrows(issue_url)
            soup = BeautifulSoup(response.text, "lxml")

            div = soup.find("div", {
                "data-groupid": group_id,
                "class": "list-of-issues__group list-of-issues__group--issues js--open"
            })

            if div:
                links = div.find_all("a", href=True)
                for link in links:
                    href = urljoin(base_url, link["href"])
                    if href not in unique_links:  
                        unique_links.add(href)
                        print(f"Found: {href}")

        except Exception as e:
            print(f"Error fetching data for group ID {group_id}: {e}")

    article_links = set()
    processed_article_ids = set()

    for issue_link in unique_links:
        try:
            print(f"Fetching: {issue_link}")
            response = fetch_page_with_zenrows(issue_link)
            page_soup = BeautifulSoup(response.text, "lxml")

            sections = page_soup.find_all("section", {"class": "toc__section"})
            for section in sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag and any(keyword in h2_tag.get_text(strip=True).lower() for keyword in ["basic research", "clinical investigation", "clincial investigation","research letters"]):
                    for li in section.find_all("li"):
                        article_link = li.find("a", href=True)
                        if article_link:
                            href = article_link["href"]
                            match = re.search(r"/(S\d+\-\d+\(\d+\)\d+\-\d+)/", href)
                            if match:
                                current_article_id = match.group(1)  
                                if current_article_id in processed_article_ids:
                                    continue
                                processed_article_ids.add(current_article_id)
                                full_article_link = urljoin(base_url, href)
                                article_links.add(full_article_link)
                                print(full_article_link)
        except Exception as e:
            print(f"Error fetching article links from {issue_link}: {e}")

    # Convert article links to a list
    article_links_list = list(article_links)

    # Insert into database
    insert_into_database(conn, name, article_links_list,specialization)

    print(f"Total unique articles found: {len(article_links_list)}")


def fetch_cjasn_articles(conn):
    name ='Clinical Journal of the American Society of Nephrology'
    base_url = "https://journals.lww.com/cjasn/toc"
    start_year = 2024
    end_year = 2021
    unique_links = set()  

    

    for year in range(start_year, end_year - 1, -1):  # Loop from 2024 to 2021
        for issue in range(1, 13):  # Loop over all 12 issues for each year
            issue_str = f"{issue:02}000"  # Format issue with leading zeros (e.g., 01000)
            issue_url = f"{base_url}/{year}/{issue_str}"
            print(f"Fetching: {issue_url}")
            try:
                # Fetch page using a helper function (e.g., ZenRows client)
                response = fetch_page_with_zenrows(issue_url)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "lxml")

                    
                    sections = soup.find_all("section", id="wp-articles-navigator", class_="content-box")

                    for section in sections:
                        header = section.find("header")
                        if header:
                            h3_tag = header.find("h3")
                            if h3_tag and any(keyword in h3_tag.getText(strip=True).lower() for keyword in ["original article","clinical research"]):                          
                                h4_tags = section.find_all("h4")
                                for h4_tag in h4_tags:
                                    link = h4_tag.find("a", href=True)  # Find <a> tags in <h4>
                                    if link:
                                        href = urljoin(issue_url, link["href"])  # Resolve relative URLs
                                        if href not in unique_links:  # Deduplicate
                                            unique_links.add(href)
                                            print(f"Found: {href}")
                        else:
                            continue
                else:
                    print(f"Failed to fetch {issue_url}: Status Code {response.status_code}")
            except Exception as e:
                print(f"Error occurred while processing {issue_url}: {e}")

    # Convert set to list to maintain ordering
    article_links = list(unique_links)
    # Convert article links to a list
    article_links_list = list(article_links)

    # Insert into database
    insert_into_database(conn, name, article_links_list,specialization)

    print(f"Total unique articles found: {len(article_links)}")


def fetch_ndt_articlies(conn):
    name = 'Nephrology Dialysis Transplantation'
    base_url = "https://academic.oup.com"
    main_url = f"{base_url}/ndt/issue"
    article_links = []
        

    # Iterate over volumes and issues
    for volume in range(39, 35, -1):  
        for issue in range(1, 13):  # Issues 1 to 12
            issue_url = f"{main_url}/{volume}/{issue}"
            print(f"Scraping issue: Volume {volume}, Issue {issue} at {issue_url}")
            
            try:
                # Fetch the issue page
                    
                response = fetch_page_with_zenrows('https://academic.oup.com/ndt/issue/36/1')
                
                soup = BeautifulSoup(response.content, "html.parser")

                # Locate the section containing Original Articles
                h4_tag = soup.find("h4", class_="title articleClientType act-header", string=lambda text: text and text.lower() == "original articles")

                if not h4_tag:
                    continue
                
                # Find the div containing article links
                content_div = h4_tag.find_next("div", class_="content al-article-list-group")
                if not content_div:
                    continue

                # Extract articles from the div
                article_divs = content_div.find_all("div", class_="al-article-item-wrap al-normal")
                for article_div in article_divs:
                    h5_tag = article_div.find("h5", class_="customLink item-title")
                    if h5_tag:
                        link = h5_tag.find("a", href=True)
                        if link:
                            full_article_link = urljoin(base_url, link["href"])
                            print(f"Found : {full_article_link}")
                            article_links.append(full_article_link)

            except requests.exceptions.RequestException as e:
                print(f"Error fetching {issue_url}: {e}")
                continue

    # Print the total number of links found
    print(f"Total links found for {name}: {len(article_links)}")

    # Insert into database
    insert_into_database(conn, name, article_links,specialization)

def fetch_akjd_articles(conn):
    name = "American Journal of Kidney Diseases"
    base_url = "https://www.ajkd.org"
    main_url = f"{base_url}/issues"
    group_ids = [f"d2020.v{num}" for num in range(84, 76, -1)] 
    unique_links = set()  
    for group_id in group_ids:
        issue_url = f"{main_url}?publicationCode=yajkd&issueGroupId={group_id}"
        print(f"Fetching : {issue_url}")
        try:
            response = fetch_page_with_zenrows(issue_url)
            soup = BeautifulSoup(response.text, "lxml")

            div = soup.find("div", {
                "data-groupid": group_id,
                "class": "list-of-issues__group list-of-issues__group--issues js--open"
            })

            if div:
                links = div.find_all("a", href=True)
                for link in links:
                    href = urljoin(base_url, link["href"])
                    if href not in unique_links:  
                        unique_links.add(href)
                        print(f"Found: {href}")

        except Exception as e:
            print(f"Error fetching data for group ID {group_id}: {e}")

    article_links = set()
    processed_article_ids = set()

    for issue_link in unique_links:
        try:
            print(f"Fetching: {issue_link}")
            response = fetch_page_with_zenrows(issue_link)
            page_soup = BeautifulSoup(response.text, "html.parser")

            sections = page_soup.find_all("section", {"class": "toc__section"})
            for section in sections:
                h2_tag = section.find("h2", class_="toc__heading__header top")
                if h2_tag and any(keyword in h2_tag.get_text(strip=True).lower() for keyword in ["special report", "original investigations"]):
                    for li in section.find_all("li"):
                        article_link = li.find("a", href=True)
                        if article_link:
                            href = article_link["href"]
                            match = re.search(r"/(S\d+\-\d+\(\d+\)\d+\-\d+)/", href)
                            if match:
                                current_article_id = match.group(1)  
                                if current_article_id in processed_article_ids:
                                    continue
                                processed_article_ids.add(current_article_id)
                                full_article_link = urljoin(base_url, href)
                                article_links.add(full_article_link)
                                print(full_article_link)
        except Exception as e:
            print(f"Error fetching article links from {issue_link}: {e}")

    # Convert article links to a list
    article_links_list = list(article_links)

    # Insert into database
    insert_into_database(conn, name, article_links_list,specialization)

    print(f"Total unique articles found: {len(article_links_list)}")

if __name__=="__main__":
    if conn:= setup_database():
        #fetch_jasn_articles(conn)
        #fetch_kidney_international_articles(conn)
        #fetch_cjasn_articles(conn)
        fetch_ndt_articlies(conn)
        #fetch_akjd_articles(conn)