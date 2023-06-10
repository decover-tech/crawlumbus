# Licensed under the MIT License.
# Copyright (c) Microsoft Corporation. All rights reserved.
import json

# -*- coding: utf-8 -*-
import logging
import os

import requests
from bs4 import BeautifulSoup

from utilities.web_page_info import WebPageInfo


# ''' This sample makes a call to the Bing Web Search API with a query and returns relevant web search.
# Documentation: https://docs.microsoft.com/en-us/bing/search-apis/bing-web-search/overview '''

def remove_html_tags(text: str) -> str:
    """Remove html tags from a string"""
    soup = BeautifulSoup(text, "html.parser")
    cleaned_text = soup.get_text()
    return cleaned_text


def extract_news_info(data: str) -> list:
    webpages = []
    for item in data['value']:
        url = item['url']
        name = remove_html_tags(item['name'])
        snippet = item['description']
        webpages.append(WebPageInfo(name, url, snippet))
    return webpages


def extract_webpage_info(data: str) -> list:
    webpages = []
    for page in data["webPages"]["value"]:
        name = page["name"]
        url = page["url"]
        snippet = page["snippet"]
        webpages.append(WebPageInfo(name, url, snippet))
    return webpages


class BingClient:
    """
    This class is used to search for a query using Bing Search API.
    """

    def __init__(self):
        self.subscription_key = os.environ.get('BING_SEARCH_V7_SUBSCRIPTION_KEY')
        self.search_url = "https://api.bing.microsoft.com/v7.0/search"
        self.news_search_url = "https://api.bing.microsoft.com/v7.0/news/search"

    def news_search(self, search_term: str, source_country: str) -> list:
        webpages = []
        print(f'Searching for {search_term}...')
        # Call the API try.
        try:
            # Construct a request.
            mkt = 'en-US' if source_country == 'United States' else 'en-IN'
            headers = {"Ocp-Apim-Subscription-Key": self.subscription_key}
            params = {"q": search_term, "textDecorations": True, "textFormat": "HTML", "mkt": mkt}
            response = requests.get(self.news_search_url, headers=headers, params=params)
            response.raise_for_status()
            search_results = response.json()

            # Print the response in a pretty way.
            return extract_news_info(search_results)

        except Exception as ex:
            logging.error(f'Exception occurred while calling Bing Search API: {ex}')
            raise ex

    def search(self, search_term: str, source_country: str) -> list:
        logging.info(f'Searching for {search_term}...')
        webpages = []
        # Call the API.
        try:
            # Construct a request.
            mkt = 'en-US' if source_country == 'United States' else 'en-IN'
            headers = {"Ocp-Apim-Subscription-Key": self.subscription_key}
            params = {"q": search_term, "textDecorations": True, "textFormat": "HTML", "mkt": mkt}
            response = requests.get(self.search_url, headers=headers, params=params)
            response.raise_for_status()
            search_results = response.json()

            # Print the response in a pretty way.
            return extract_webpage_info(search_results)

        except Exception as ex:
            logging.error(f'Exception occurred while calling Bing Search API: {ex}')
            raise ex