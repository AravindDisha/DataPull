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
from os import walk
import re
import json
# use concurrent.futures to parallelize

home = Path.home()
path_to_ct_keys_yaml = './files/crowdtangle_keys.yaml'
path_to_twit_keys_yaml = './files/twitter_keys.yaml'
path_to_keywords_yaml = "./files/keywords.yaml"
path_to_exportcomments_keys = './files/exportcomments_keys.yaml'
path_to_data = "/PulledData"
path_to_new_extractions = "/PulledData/data_{sm_type}_{from_date}_{to_date}.pkl"


# path_to_new_extractions = "./Data/data_{sm_type}_{from_date}_{to_date}.pkl" # comment this before pushing
# path_to_data = "./Data" # comment this before pushing

app = Flask(__name__)


@app.route('/')
def index():
    print('Request for index page received')
    return render_template('index.html',  url_for_list=url_for('get_extracted_list'), url_for_extraction=url_for('collect_comments_post'), url_for_fb_list=url_for('get_facebook_dfs'), url_for_extracted=url_for('get_comment_dfs'))

@app.route('/listExtracted')
def get_extracted_list():
    f = []
    d1 = {'tweet_keyword':[],'insta_post':[],'fb_post':[],'tweet_location':[],'comment_post':[]}
    for (dirpath, dirnames, filenames) in walk(path_to_data):
        f.extend(filenames)
    pat = "[a-zA-Z0-9\.]+"
    for fname in f:
        t = re.findall(pat,fname)
        if('sm' not in t and 'comment' not in t):
            d1[t[1]+'_'+t[2]].append(datetime.fromtimestamp(float(t[3])).strftime("%d %b, %Y")+" - "+datetime.fromtimestamp(float(t[4][:-4])).strftime("%d %b, %Y"))
    return json.dumps(d1, indent = 4)

@app.route('/listFacebookDfs')
def get_facebook_dfs():
    f = []
    for (dirpath, dirnames, filenames) in walk(path_to_data):
        f.extend(filenames)
    pat = "[a-zA-Z0-9\.]+"
    l = []
    for fname in f:
        t = re.findall(pat,fname)
        if(t[1] == 'fb'):
            l.append({'name':datetime.fromtimestamp(float(t[3])).strftime("%d %b, %Y")+" - "+datetime.fromtimestamp(float(t[4][:-4])).strftime("%d %b, %Y"), 'value':fname});
    return json.dumps(l)

@app.route('/listExtractedComments')
def get_comment_dfs():
    f = []
    for (dirpath, dirnames, filenames) in walk(path_to_data):
        f.extend(filenames)
    pat = "[a-zA-Z0-9\.]+"
    l = []
    for fname in f:
        t = re.findall(pat,fname)
        if(t[1] == 'comment'):
            l.append(datetime.fromtimestamp(float(t[3])).strftime("%d %b, %Y")+" - "+datetime.fromtimestamp(float(t[4][:-4])).strftime("%d %b, %Y"))
    return 'Extracted comments: \n'+'\n'.join(l)

@app.route('/fetchComments', methods=['POST'])
def fetch_comment():
    df_name = request.form.get('df_name')
    url_for_extraction = url_for('collect_comments_post',df_name=df_name)
    pat = "[a-zA-Z0-9\.]+"
    t = re.findall(pat,df_name)
    return render_template('hello.html', etype='comments', sdate=datetime.fromtimestamp(float(t[3])).strftime("%d %b, %Y"), edate=datetime.fromtimestamp(float(t[4][:-4])).strftime("%d %b, %Y"), url_for_extraction=url_for_extraction)

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
        # send url for logging instead and move date code and calling extraction code there.
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
        sm_type="tweet_location", from_date=start_date.timestamp(), to_date=end_date.timestamp())
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
    path_to_storage_file = path_to_new_extractions.format(
        sm_type="insta_post", from_date=start_date.timestamp(), to_date=end_date.timestamp())
    insta_df = None
    try:
        insta_df = get_insta_posts(
            path_to_keywords_yaml, path_to_ct_keys_yaml, start_date, end_date)
        f = open(path_to_storage_file, 'wb')
        print("Dumping insta data to pkl")
        pickle.dump(insta_df, f)
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
    path_to_storage_file = path_to_new_extractions.format(
        sm_type="fb_post", from_date=start_date.timestamp(), to_date=end_date.timestamp())
    fb_df = None
    try:
        fb_df = get_fb_posts(
            path_to_keywords_yaml, path_to_ct_keys_yaml, start_date, end_date)
        f = open(path_to_storage_file, 'wb')
        print("Dumping fb data to pkl")
        pickle.dump(fb_df, f)
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

# make different api call with date selction 
# upload extracted and call from last extraction
@app.route('/fetchFacebookComments', methods=['GET'])
def collect_comments_post():
    # append _comments to the file
    args = request.args
    fname = args.get('df_name')
    sm_df = pd.read_pickle(path_to_data+'/'+fname)
    new_fname2 = path_to_data+'/data_comment_'+fname[fname.find('post'):]
    df2 = None
    new_fname1 = path_to_data+'/sm_data_'+fname[fname.find('post_')+5:]
    df1 = None
    i = 0
    while i < sm_df.shape[0]:
        print("comment pull number "+str(i))
        comments_output = get_post_comments(sm_df[sm_df.platform!='twitter'].iloc[i:i+5], path_to_exportcomments_keys)
        if(df1 === None):
            df1 = comments_output[0]
        else:
            df1 = df1.append(comments_output[0])
        f = open(new_fname1, 'wb')
        pickle.dump(df1,f)
        print("dumped into file 1")
        f.close() # update the same file
        if(df2 === None):
            df2 = comments_output[1]
        else:
            df2 = df2.append(comments_output[1])
        f1 = open(new_fname2, 'wb')
        pickle.dump(df2,f1)
        print("dumped into file 2")
        f1.close()
        i+=5
    if(i == sm_df.shape[0]):
        return 'Successfully extracted to '+new_fname1+' and '+ new_fname2
    else:
        return 'Wrote partial data '+str(i)+'/'+str(sm_df.shape[0])+' extracted to '+new_fname1+' and '+ new_fname2

if __name__ == '__main__':
    app.run()
