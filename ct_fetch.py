import requests
from requests_oauthlib import OAuth1Session
from pytangle.api import API

from pathlib import Path

import pandas as pd
import yaml
import numpy as np
from datetime import datetime

import matplotlib.pyplot as plt
import seaborn as sns

# Inputs:

# Start date: currently arbitrarily hard-coded; fed to the API as a date
# End date: datetime.today(); fed to the API as a date (i.e. without a specified timestamp)
# COVID keywords: COVID_KEYWORDS from 'files/keywords.yaml'
# API tokens: from home/'.crowdtangle_keys.yaml' (which each user should have on their system, not synced on Dropbox)
# Outputs:

# fb_posts: list of posts pulled from CT FB lists, where each post's data is a dict
# ig_posts: ditto, for CT IG lists

# Using pytangle (https://github.com/hide-ous/pytangle) wrapper for CT API to deal with pagination, rate limits, etc.



def get_insta_posts(path_to_keywords_yaml, path_to_ct_keys_yaml, start_date, end_date):
    # SETUP
    with open(path_to_keywords_yaml) as f:
        keywords = yaml.safe_load(f)
    keywords_covid = keywords['COVID_KEYWORDS'] # list of keyword strings
    with open(path_to_ct_keys_yaml) as f:
        ct_keys = yaml.safe_load(f)
       
    # Get token
    ig_token = ct_keys['instagram']['token']
    
    # Getting details for lists
    r = requests.get('https://api.crowdtangle.com/lists', params={'token': ig_token})
    
    ig_lists = r.json()
    # Getting the lists we want to ultimately use, i.e. prefixed with RADx-
    ig_lists_select = [l for l in ig_lists['result']['lists'] if l['title'].startswith("RADx")]
    accounts = get_accounts_with_lists(ig_token, ig_lists_select)
    # Initiating Pytangle API
    ig_api = API(token = ig_token)
    
    # Setting up our API request with params
    ig_posts_pull = ig_api.posts(
    #     platforms = 'facebook',
        listIds = [l['id'] for l in ig_lists_select],
        searchTerm = ','.join(w for w in keywords_covid),
        startDate = start_date.date().strftime('%Y-%m-%d'),
        endDate = end_date.date().strftime('%Y-%m-%d'),
        sortBy = 'date',
        count = -1
    )
    
    # Do API pull
    ig_posts = []
    for n, a_post in enumerate(ig_posts_pull):
        # Progress check print-out
        if not n % 1000:
            print(n)
        # Add post to list
        ig_posts.append(parse_ct_post(a_post, keywords['COVID_TOPICS'], accounts))
    return pd.DataFrame(ig_posts)


def get_fb_posts(path_to_keywords_yaml, path_to_ct_keys_yaml, start_date, end_date):
    with open(path_to_keywords_yaml) as f:
        keywords = yaml.safe_load(f)
    keywords_covid = keywords['COVID_KEYWORDS'] # list of keyword strings
    with open(path_to_ct_keys_yaml) as f:
        ct_keys = yaml.safe_load(f)
        
        
    fb_token = ct_keys['facebook']['token']
    r = requests.get('https://api.crowdtangle.com/lists', params={'token': fb_token})
    fb_lists = r.json()
    # Getting the lists we want to ultimately use, i.e. prefixed with RADx-
    fb_lists_select = [l for l in fb_lists['result']['lists'] if l['title'].startswith("RADx")]
    # Initiating Pytangle API
    fb_api = API(token = fb_token)
    # Setting up our API request with params
    fb_posts_pull = fb_api.posts(
    #     platforms = 'facebook',
        listIds = [l['id'] for l in fb_lists_select],
        searchTerm = ','.join(w for w in keywords_covid),
        startDate = start_date.date().strftime('%Y-%m-%d'),
        endDate = end_date.date().strftime('%Y-%m-%d'),
        sortBy = 'date',
        count = -1
    )
    
    accounts = get_accounts_with_lists(fb_token, fb_lists_select)
    # Do API pull
    fb_posts = []
    for n, a_post in enumerate(fb_posts_pull):
        # Progress check print-out
        if not n % 1000:
            print(n)
        # Add post to list
        fb_posts.append(parse_ct_post(a_post, keywords['COVID_TOPICS'], accounts))
   
    return pd.DataFrame(fb_posts)

def parse_ct_post(raw_post, topic_dict, account_with_lists):
    platform = raw_post["platform"].lower()
    if platform == "instagram":
        try:
            content = raw_post["description"]
        except Exception:
            content = ""
        author = raw_post['account']["handle"]
    if platform == "facebook":
        content = ""
        try:
            content = raw_post["message"]
        except Exception:
            pass
        try:
            content = content + " " + raw_post["description"]
        except Exception:
            pass
        author = raw_post['account']["name"]
    engagement_raw = sum([post for post in raw_post["statistics"]["actual"].values()])
    engagement_normed = 0 if raw_post['subscriberCount'] == 0 else engagement_raw / raw_post['subscriberCount']
#     print([account_with_lists.items()][0])
    try:
        labels = account_with_lists[raw_post['account']["id"]]['lists']
#         print(labels)
    except Exception:
        labels = []
        print("something happened")
        print(raw_post)
    return {
        "author": author,
        "url": raw_post["postUrl"],
        "platform": platform,
        "content": content,
        "topics": match_topics(content, topic_dict),
        "labels": labels,
        "engagementRaw": engagement_raw,
        "engagementNormed": engagement_normed,
        "fetchedAt": datetime.now(),
        "authoredAt": raw_post["date"],
        "platformID": raw_post["id"],
        "raw": raw_post
    }


def match_topics(content, topic_dict):
    found_topics = []
    for topic, topic_keywords in topic_dict.items():
        if any(word in content.lower() for word in topic_keywords):
            found_topics = found_topics + [topic]
    return found_topics

def get_accounts_with_lists(token, lists):
        # Getting ig accounts (for labelling)
    ig_accounts = []
    unique_accounts_by_list = {}
    print(lists)
    for l in lists:
        listId = str(l['id'])
        listName = l['title']
        params = {
            'token': token,
            'count': 100
        }
        r = requests.get('https://api.crowdtangle.com/lists/' + listId + "/accounts", params = params)
        accounts = r.json()
        ig_accounts = ig_accounts + accounts['result']['accounts']
        for user in accounts['result']['accounts']:
#             print(user)
            if user['id'] in unique_accounts_by_list:
                unique_accounts_by_list[user['id']]['lists'].append(listName[5:].lower())
            else:
                unique_accounts_by_list[user['id']] = user
                unique_accounts_by_list[user['id']]['lists'] = [listName[5:].lower()]
    return unique_accounts_by_list


