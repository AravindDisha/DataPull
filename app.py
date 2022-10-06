import importlib
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
from twitter_fetch import get_twitter_posts_from_lists, get_twitter_posts_with_location
import exportcomments_fetch
from exportcomments_fetch import get_post_comments
importlib.reload(exportcomments_fetch)
from ct_fetch import get_insta_posts, get_fb_posts
import pandas as pd
from dateutil.relativedelta import relativedelta
from pathlib import Path
import traceback
import pickle
# use concurrent.futures to parallelize

home = Path.home()
path_to_ct_keys_yaml = './files/crowdtangle_keys.yaml'
path_to_twit_keys_yaml = './files/twitter_keys.yaml'
path_to_keywords_yaml = "./files/keywords.yaml"
path_to_exportcomments_keys = './files/exportcomments_keys.yaml'
path_to_new_extractions = "/PulledData/data_{sm_type}_{from_date}_{to_date}.pkl"
path_to_pre_comment_extractions = "/PulledData/Raw/data_{sm_type}_{from_date}_{to_date}.pkl"

# path_to_new_extractions = "./Data/data_{sm_type}_{from_date}_{to_date}.pkl" # comment this before pushing
# path_to_pre_comment_extractions = "./Data/Raw/data_{sm_type}_{from_date}_{to_date}.pkl" # comment this before pushing

app = Flask(__name__)


@app.route('/')
def index():
    print('Request for index page received')
    return render_template('index.html')


@app.route('/fetch', methods=['POST'])
def fetch():
    print('Request for data fetch received')
    extraction_type = request.form.get('extraction_type')
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    if extraction_type:
        print('Request for fetch page received with:')
        print('extraction_type=%s' % extraction_type)
        print('start_date="%s"' % start_date)
        print('end_date="%s"' % end_date)
        url_for_extraction = '/'

        if (end_date == ''):
            end_date = datetime.now().strftime("%Y-%m-%d")
        if (start_date == ''):
            start_date = (datetime.now() - relativedelta(days=7) # change days to 7 before uploading
                          ).strftime("%Y-%m-%d")

        if (extraction_type == 'tweet_keyword'):
            url_for_extraction = url_for('collect_tweets_keyword',start_date=start_date,end_date=end_date)
        elif (extraction_type == 'insta_post'):
            url_for_extraction = url_for('collect_insta_keyword',start_date=start_date,end_date=end_date)
        elif (extraction_type == 'fb_post'):
            url_for_extraction = url_for('collect_facebook_keyword',start_date=start_date,end_date=end_date)
        elif (extraction_type == 'tweet_location'):
            url_for_extraction = url_for('collect_tweet_location',start_date=start_date,end_date=end_date)
        return render_template('hello.html', etype=extraction_type, sdate=start_date, edate=end_date, url_for_extraction=url_for_extraction)
    else:
        print('Request for extraction invalid -- redirecting')
        return redirect(url_for('index'))


@app.route('/fetchTweetsKeywords', methods=['GET'])
def collect_tweets_keyword():
    # collecting args of GET request to get start and end dates of tweets to be collected
    args = request.args
    end_date = datetime.strptime(args.get('amp;end_date'), "%Y-%m-%d") # amp; is a temporary fix due to string conversion errors
    start_date = datetime.strptime(args.get('start_date'), "%Y-%m-%d")
    print('Extraction of tweet by keyword started')

    # from here - move to a concurrent thread using ThreadPoolExecutor and send output as log. Assign Job IDs?
    # naming for file to store extraction from a certain date to a certain date
    path_to_storage_file = path_to_new_extractions.format(
        sm_type="tweet_keyword", from_date=start_date.timestamp(), to_date=end_date.timestamp())
    twit_df = None
    try:
        twit_df = get_twitter_posts_from_lists(
            path_to_keywords_yaml, path_to_twit_keys_yaml, start_date, end_date)
        print("Dumping data to pkl")
        f = open(path_to_storage_file, 'wb')
        pickle.dump(twit_df, f)
        f.close()
    except:
        tb = traceback.format_exc()
        error_str = 'Twitter Keyword Fetch failed with error \n{tb}\n'.format(
            tb=tb)
        # find if the file was accidentally half-made and delete
        print(error_str)
        return error_str
    else:
        success_str = 'Wrote twitter keyword extraction from {from_date} to {to_date} in {fname}'.format(
            from_date=start_date.strftime("%m/%d/%Y, %H:%M:%S"), to_date=end_date.strftime("%m/%d/%Y, %H:%M:%S"), fname=path_to_storage_file)
        print(success_str)
        return '<h3>'+success_str+'</h3><br/>'+(twit_df.sample(5)[["author", "platform", "content"]]).to_html()

@app.route('/fetchTweetsLocation', methods=['GET'])
def collect_tweet_location():
    # collecting args of GET request to get start and end dates of tweets to be collected
    args = request.args
    end_date = datetime.strptime(args.get('amp;end_date'), "%Y-%m-%d") # amp; is a temporary fix due to string conversion errors
    start_date = datetime.strptime(args.get('start_date'), "%Y-%m-%d")
    print('Extraction of tweet by location started')

    # from here - move to a concurrent thread using ThreadPoolExecutor and send output as log. Assign Job IDs?
    # naming for file to store extraction from a certain date to a certain date
    path_to_storage_file = path_to_new_extractions.format(
        sm_type="tweet_keyword", from_date=start_date.timestamp(), to_date=end_date.timestamp())
    twit_df = None
    try:
        twit_df = get_twitter_posts_with_location("atlanta",
            path_to_keywords_yaml, path_to_twit_keys_yaml, start_date, end_date)
        print("Dumping data to pkl")
        f = open(path_to_storage_file, 'wb')
        pickle.dump(twit_df, f)
        f.close()
    except:
        tb = traceback.format_exc()
        error_str = 'Twitter Location Fetch failed with error \n{tb}\n'.format(
            tb=tb)
        # find if the file was accidentally half-made and delete
        print(error_str)
        return error_str
    else:
        success_str = 'Wrote twitter location extraction from {from_date} to {to_date} in {fname}'.format(
            from_date=start_date.strftime("%m/%d/%Y, %H:%M:%S"), to_date=end_date.strftime("%m/%d/%Y, %H:%M:%S"), fname=path_to_storage_file)
        print(success_str)
        return '<h3>'+success_str+'</h3><br/>'+(twit_df.sample(5)[["author", "platform", "content"]]).to_html()

@app.route('/fetchInstaPosts', methods=['GET'])
def collect_insta_keyword():
    # collecting args of GET request to get start and end dates of tweets to be collected
    args = request.args
    end_date = datetime.strptime(args.get('amp;end_date'), "%Y-%m-%d") # amp; is a temporary fix due to string conversion errors
    start_date = datetime.strptime(args.get('start_date'), "%Y-%m-%d")
    print('Extraction of insta post by keyword started')

    # from here - move to a concurrent thread using ThreadPoolExecutor and send output as log. Assign Job IDs?
    # naming for file to store extraction from a certain date to a certain date
    path_to_storage_file = path_to_pre_comment_extractions.format(
        sm_type="insta_post", from_date=start_date.timestamp(), to_date=end_date.timestamp())
    insta_df = None
    try:
        insta_df = get_insta_posts(
            path_to_keywords_yaml, path_to_ct_keys_yaml, start_date, end_date)
        f = open(path_to_storage_file, 'wb')
        print("Dumping insta data to pkl 1")
        pickle.dump(insta_df, f)
        print("Initiating insta comment pull")
        collect_comments_post(insta_df,start_date,end_date)
        print("Done pulling insta comments")
        f.close()
    except:
        tb = traceback.format_exc()
        error_str = 'Insta Post Fetch failed with error \n{tb}\n'.format(
            tb=tb)
        # find if the file was accidentally half-made and delete
        print(error_str)
        return error_str
    else:
        success_str = 'Wrote insta post extraction from {from_date} to {to_date} in {fname}'.format(
            from_date=start_date.strftime("%m/%d/%Y, %H:%M:%S"), to_date=end_date.strftime("%m/%d/%Y, %H:%M:%S"), fname=path_to_storage_file)
        print(success_str)
        return '<h3>'+success_str+'</h3><br/>'+(insta_df.sample(5)[["author", "platform", "content"]]).to_html()



@app.route('/fetchFacebookPosts', methods=['GET'])
def collect_facebook_keyword():
    # collecting args of GET request to get start and end dates of tweets to be collected
    args = request.args
    end_date = datetime.strptime(args.get('amp;end_date'), "%Y-%m-%d") # amp; is a temporary fix due to string conversion errors
    start_date = datetime.strptime(args.get('start_date'), "%Y-%m-%d")
    print('Extraction of facebook post by keyword started')

    # from here - move to a concurrent thread using ThreadPoolExecutor and send output as log. Assign Job IDs?
    # naming for file to store extraction from a certain date to a certain date
    path_to_storage_file = path_to_pre_comment_extractions.format(
        sm_type="fb_post", from_date=start_date.timestamp(), to_date=end_date.timestamp())
    fb_df = None
    try:
        fb_df = get_fb_posts(
            path_to_keywords_yaml, path_to_ct_keys_yaml, start_date, end_date)
        f = open(path_to_storage_file, 'wb')
        print("Dumping fb data to pkl 1")
        pickle.dump(fb_df, f)
        print("Initiating fb comment pull")
        collect_comments_post(fb_df,start_date,end_date)
        print("Done pulling fb comments")
        f.close()
    except:
        tb = traceback.format_exc()
        error_str = 'Facebook Post Fetch failed with error \n{tb}\n'.format(
            tb=tb)
        # find if the file was accidentally half-made and delete
        print(error_str)
        return error_str
    else:
        success_str = 'Wrote facebook post extraction from {from_date} to {to_date} in {fname}'.format(
            from_date=start_date.strftime("%m/%d/%Y, %H:%M:%S"), to_date=end_date.strftime("%m/%d/%Y, %H:%M:%S"), fname=path_to_storage_file)
        print(success_str)
        return '<h3>'+success_str+'</h3><br/>'+(fb_df.sample(5)[["author", "platform", "content"]]).to_html()

def collect_comments_post(sm_df,start_date,end_date):
    # append _comments to the file
    i = 0
    while i < sm_df.shape[0]:
        print("comment pull number "+str(i))
        comments_output = get_post_comments(sm_df[sm_df.platform!='twitter'].iloc[i:i+60], path_to_exportcomments_keys)
        f = open(path_to_pre_comment_extractions.format(sm_type="sm_post_"+str(int(i/60)+1), from_date=start_date.timestamp(), to_date=end_date.timestamp()), 'wb')
        pickle.dump(comments_output[0],f)
        f.close()
        f1 = open(path_to_pre_comment_extractions.format(sm_type="comment_post_"+str(int(i/100)+1), from_date=start_date.timestamp(), to_date=end_date.timestamp()), 'wb')
        pickle.dump(comments_output[1],f1)
        f1.close()
        i+=60

if __name__ == '__main__':
    app.run()
