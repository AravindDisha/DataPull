from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
from twitter_fetch import get_twitter_posts_from_lists, get_twitter_posts_with_location
from ct_fetch import get_insta_posts, get_fb_posts
import pandas as pd
from dateutil.relativedelta import relativedelta
from pathlib import Path
import traceback
import os
import pickle
# use concurrent.futures to parallelize

home = Path.home()
path_to_ct_keys_yaml = './files/crowdtangle_keys.yaml'
path_to_twit_keys_yaml = './files/twitter_keys.yaml'
path_to_keywords_yaml = "./files/keywords.yaml"
path_to_new_extractions = "/PulledData/data_{sm_type}_{from_date}_{to_date}.pkl"
# comment this before pushing
path_to_new_extractions = "./Data/data_{sm_type}_{from_date}_{to_date}.pkl"

app = Flask(__name__)


@app.route('/')
def index():
    print('Request for index page received')
    return render_template('index.html')


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')


@app.route('/fetch', methods=['POST'])
def fetch():
    extraction_type = request.form.get('extraction_type')
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    if extraction_type:
        print('Request for fetch page received with extraction_type=%s' %
              extraction_type)
        print('start_date=%s' % start_date)
        print('end_date=%s' % end_date)
        url_for_extraction = '#'
        if (extraction_type == 'tweet_keyword'):
            url_for_extraction = url_for('collect_tweets_keyword')
        return render_template('hello.html', etype=extraction_type, sdate=start_date, edate=end_date, url_for_extraction=url_for_extraction)
    else:
        print('Request for extraction invalid -- redirecting')
        return redirect(url_for('index'))


@app.route('/fetchTweetsKeywords', methods=['GET'])
def collect_tweets_keyword():
    # collecting args of GET request to get start and end dates of tweets to be collected
    dates = request.args.to_dict()
    if 'end_date' in dates.keys():
        # edit according to incoming format
        end_date = datetime.strptime(dates['end_date'], '%Y-%m-%d').date()
    else:
        end_date = datetime.now()

    if 'start_date' in dates.keys():
        # edit according to incoming format
        start_date = datetime.strptime(dates['start_date'], '%Y-%m-%d').date()
    else:
        start_date = end_date - relativedelta(days=7)

    # from here - move to a concurrent thread using ThreadPoolExecutor and send output as log. Assign Job IDs? 
    # naming for file to store extraction from a certain date to a certain date
    path_to_storage_file = path_to_new_extractions.format(
        sm_type="tweet_keyword", from_date=start_date.timestamp(), to_date=end_date.timestamp())
    twit_df = None
    try:
        twit_df = get_twitter_posts_from_lists(
            path_to_keywords_yaml, path_to_twit_keys_yaml, start_date, end_date)
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


if __name__ == '__main__':
    app.run()
