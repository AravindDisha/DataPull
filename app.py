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

@app.route('/hello', methods=['POST'])
def hello():
   name = request.form.get('name')

   if name:
       print('Request for hello page received with name=%s' % name)
       return render_template('hello.html', name = name)
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))


@app.route('/fetchTweetsKeywords', methods=['GET'])
def collect_tweets_keyword():
    # collecting args of GET request to get start and end dates of tweets to be collected
    dates = request.args.to_dict()
    if 'end_date' in dates.keys():
         end_date = datetime.fromtimestamp(int(dates['end_date'])) # edit according to incoming format
    else:
         end_date = datetime.now()
    
    if 'start_date' in dates.keys():
         start_date = datetime.fromtimestamp(int(dates['start_date'])) # edit according to incoming format
    else:
        start_date = end_date - relativedelta(days=7)
    
    # name for file to store extraction from a certain date to a certain date
    path_to_storage_file = path_to_new_extractions.format(sm_type = "tweet_keyword", from_date = start_date.timestamp(), to_date = end_date.timestamp())
    twit_df = None
    try:
        twit_df = get_twitter_posts_from_lists(path_to_keywords_yaml, path_to_twit_keys_yaml, start_date, end_date)
        f = open(path_to_storage_file,'wb')
        pickle.dump(twit_df,f)
        f.close()
    except: 
        tb = traceback.format_exc()
        error_str = 'Twitter Keyword Fetch failed with error \n{tb}\n'.format(tb = tb)
        # find if the file was accidentally half-made and delete
        print(error_str)
        return error_str
    else:
        success_str = 'Wrote twitter keyword extraction from {from_date} to {to_date} in {fname}'.format(from_date = start_date.strftime("%m/%d/%Y, %H:%M:%S"), to_date = end_date.strftime("%m/%d/%Y, %H:%M:%S"), fname = path_to_storage_file)
        print(success_str)
        return '<h3>'+success_str+'</h3><br/>'+(twit_df.sample(5)[["author","platform","content"]]).to_html()

if __name__ == '__main__':
   app.run()