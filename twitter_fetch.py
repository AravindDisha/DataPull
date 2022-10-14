# Import the tweepy package
from asyncio.windows_events import NULL
import tweepy

# Import a variety of other packages that may be useful for working with data.
import pandas as pd
import json
import time
import yaml
from requests_oauthlib import OAuth1Session

from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_twitter_lists(path_to_twit_keys_yaml):
    with open(path_to_twit_keys_yaml) as f:
        tw_keys = yaml.safe_load(f)
    twitter = OAuth1Session(tw_keys['get_lists']['consumer_key'],
                            client_secret=tw_keys['get_lists']['consumer_secret'],
                            resource_owner_key=tw_keys['get_lists']['access_token'],
                            resource_owner_secret=tw_keys['get_lists']['access_token_secret'])
    # Getting details for lists
    r = twitter.get('https://api.twitter.com/1.1/lists/list.json', params={'screen_name': 'amyzlc'})
    tw_lists = r.json()
    # Getting the RADx lists
    tw_lists_select = [l for l in tw_lists if l['name'].startswith("RADx")]
    # Getting accounts for the RADx lists
    tw_accounts = []
    unique_tw_accounts_by_list = {}

    for l in tw_lists_select:
        listId = l['id']
        listName = l['name']
        params = {
            'list_id': listId,
            'count': 1000
        }
        r = twitter.get('https://api.twitter.com/1.1/lists/members.json', params = params)
        members = r.json()
        tw_accounts = tw_accounts + members['users']
        for user in members['users']:
            if user['id'] in unique_tw_accounts_by_list:
                unique_tw_accounts_by_list[user['id']]['lists'].append(listName[5:].lower())
            else:
                unique_tw_accounts_by_list[user['id']] = user
                unique_tw_accounts_by_list[user['id']]['lists'] = [listName[5:].lower()]
    tw_df = pd.json_normalize(list(unique_tw_accounts_by_list.values()))
    tw_df.insert(0, 'platform', "Twitter")
    return (tw_df, unique_tw_accounts_by_list)

def initialize_client(path_to_twit_keys_yaml):
    with open(path_to_twit_keys_yaml, 'r') as file:
        config = yaml.safe_load(file)

    my_bearer_token = config["search_tweets_all"]["bearer_token"]
    my_consumer_key = config['get_lists']['consumer_key']
    my_consumer_secret = config['get_lists']['consumer_secret']

    my_access_token = config['get_lists']['access_token']
    my_access_secret = config['get_lists']['access_token_secret']

    # Using the tweepy.Client(...) function, you can establish a connection to the
    # Twitter API.  The example below shows a robust way of creating a "client" object
    #  by passing your credentials to appropriate parameters.

    # Additionally, the "wait_on_rate_limit" parameter is set to True at this stage.
    # This will be explained more later, but this helps to overcome a lot of errors
    #  associated with API usage limitations.
    client = tweepy.Client(
        wait_on_rate_limit=True,
        consumer_key=my_consumer_key,
        consumer_secret=my_consumer_secret,
        access_token=my_access_token,
        access_token_secret=my_access_secret,
        bearer_token=my_bearer_token,
    )
    return client


def get_twitter_posts_from_lists(path_to_keywords_yaml, path_to_twit_keys_yaml, start_date, end_date):
    client = initialize_client(path_to_twit_keys_yaml)
    # Get COVID keywords from YAML
    with open(path_to_keywords_yaml, 'r') as file:
        keywords = yaml.safe_load(file)

    # Get Twitter account lists (generated by Amy's script)
#     users = pd.read_pickle(path_to_twitter_acc_pkl)
    users, user_dict = get_twitter_lists(path_to_twit_keys_yaml)
    base_query = " OR ".join(keywords["COVID_KEYWORDS"])

    base_query = "(" + base_query + ")"
    # Iterate through user IDs, create query string
    final_query = base_query
    total = 0
    cumulative_tweet_df = pd.DataFrame()
    for index, row in users.iterrows():
        candidate_user = row.screen_name
        if index == 0:
            final_query = final_query + " (from:" + candidate_user
            continue
        if len(final_query) + len(candidate_user) < (1024 - len(" OR from:)")):
            final_query = final_query + " OR from:" + candidate_user
        else:
            final_query = final_query + ")"
            tidy_twi_df = get_all_tweets_with_query(final_query, keywords['COVID_TOPICS'], client, user_dict=user_dict, start_time=start_date, end_time=end_date)
            final_query = base_query + " (from:" + candidate_user
            cumulative_tweet_df = pd.concat([cumulative_tweet_df, tidy_twi_df], ignore_index=True)
    return cumulative_tweet_df



def get_twitter_posts_with_location(location, path_to_keywords_yaml, path_to_twit_keys_yaml, start_date, end_date):
    client = initialize_client(path_to_twit_keys_yaml)
    # Get COVID keywords from YAML
    with open(path_to_keywords_yaml, 'r') as file:
        keywords = yaml.safe_load(file)

    base_query = " OR ".join(keywords["COVID_KEYWORDS"])

    base_query = "(" + base_query + ")"
    expansions = []
    # Iterate through user IDs, create query string
    final_query = base_query + "place:" + location
    tidy_twi_df = get_all_tweets_with_query(final_query, keywords['COVID_TOPICS'], client=client, start_time=start_date, end_time=end_date)
    tidy_twi_df['isLocationPulled'] = True
    tidy_twi_df['Location'] = location
    return tidy_twi_df







# If this runs really slowly, implement aho corasick (will make it 40x faster probably)
# import ahocorasick
# def make_automaton(topic_dict):
#     automaton = ahocorasick.Automaton()
#     for key, value in topic_dict.items():
#         automaton.add_word(key, (key, value))
# Return a list of topics of a post given dict of {topic1:[keyword1, keyword2], topic2: [keyword3,...]}
def match_topics(content, topic_dict):
    found_topics = []
    for topic, topic_keywords in topic_dict.items():
        if any(word in content.lower() for word in topic_keywords):
            found_topics = found_topics + [topic]
    return found_topics

# We expect at least the author_id expansion
def get_all_tweets_with_query(query, topic_dict, client, user_dict = {},
                              user_fields = ['username', 'public_metrics', 'description', 'location'],
                              tweet_fields = ['created_at', 'geo', 'public_metrics', 'text'],
                              place_fields = ["contained_within", "place_type", "full_name"],
                              expansions = ['author_id', 'geo.place_id'],
                              start_time = datetime.now() - relativedelta(years=1),
                              end_time = datetime.now() - relativedelta(days=1)):
    hoax_tweets = []
    for response in tweepy.Paginator(client.search_all_tweets,
                                     query = query,
                                     user_fields = user_fields,
                                     tweet_fields = tweet_fields,
                                     place_fields = place_fields,
                                     expansions = expansions,
                                     start_time = start_time,
                                     end_time = end_time,
                                  max_results=500):
        time.sleep(1)
        if response.meta['result_count'] > 0:
            hoax_tweets.append(response)
    result = []
    place_dict = {}
    # Loop through each response object
    for response in hoax_tweets:

        # Take all of the users, and put them into a dictionary of dictionaries with the info we want to keep
        for user in response.includes['users']:
            try:
                user_dict[user.id].update({'username': user.username,
                                  'followers': user.public_metrics['followers_count'],
                                  'tweets': user.public_metrics['tweet_count'],
                                  'description': user.description,
                                  'location': user.location
                                 })
            except Exception:
                user_dict[user.id] = {'username': user.username,
                                  'followers': user.public_metrics['followers_count'],
                                  'tweets': user.public_metrics['tweet_count'],
                                  'description': user.description,
                                  'location': user.location
                                 }
#         print(response.includes)
#         print(user_dict)
#         for place in response.includes['places']:
#             place_dict[place.id] = place
        print("[twitter_fetch] Storing " +str(len(response.data))+ " tweets with author info")
        for tweet in response.data:
            # For each tweet, find the author's information
            author_info = user_dict[tweet.author_id]
            # Calc engagement (can scale/weight these differently)
            engagement_raw = tweet.public_metrics['retweet_count'] + tweet.public_metrics['reply_count'] + tweet.public_metrics['like_count'] + tweet.public_metrics['quote_count']
            # Put all of the information we want to keep in a single dictionary for each tweet
           
            result.append({
                "authoredAt": tweet.created_at,
                "fetchedAt": datetime.now(),
                "author": author_info['username'],
                "url": "https://twitter.com/twitter/status/" + str(tweet.id),
                "platform": 'twitter',
                "platformID": tweet.id,
                "content": tweet.text,
                "topics": match_topics(tweet.text, topic_dict),
                "labels": [],
                "engagementRaw": engagement_raw,
                "engagementNormed": 0 if author_info['followers'] == 0 else engagement_raw / author_info['followers'],
                "raw": {
                    'author_id': tweet.author_id,
                   'username': author_info['username'],
                   'author_followers': author_info['followers'],
                   'author_tweets': author_info['tweets'],
                   'author_description': author_info['description'],
                   'author_location': author_info['location'],
                   'text': tweet.text,
                   'tweet_id': tweet.id,
                   'url': "https://twitter.com/twitter/status/" + str(tweet.id),
                   'created_at': tweet.created_at,
                   'retweets': tweet.public_metrics['retweet_count'],
                   'replies': tweet.public_metrics['reply_count'],
                   'likes': tweet.public_metrics['like_count'],
                   'quote_count': tweet.public_metrics['quote_count']
              }})

    # Change this list of dictionaries into a dataframe
    df = pd.DataFrame(result)
    return df