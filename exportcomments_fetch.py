import pandas as pd
import yaml
import json
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

from pathlib import Path
import urllib
import time

from exportcomments import ExportComments
# Documentation here: https://github.com/exportcomments/exportcomments-python

# Inputs:

# path_to_ex_keys_yaml: Path to API tokens: from home/'.exportcomments_keys.yaml' (which each user should have on their system, not synced on Dropbox)
# posts_df: Dataframe with rows of all posts we'd like to pull comments for (function limits to FB and IG)

# Outputs - as a tuple:

# posts_df_comments: Same dataframe of posts inputted, with new/updated column commentCountExported
# comments_df: Dataframe of comments data

def get_post_comments(posts_df, path_to_ex_keys_yaml):
    # SETUP
    with open(path_to_ex_keys_yaml) as f:
        ex_keys = yaml.safe_load(f)
    
    # Create newPostID as platform + platform ID
    posts_df['newPostID'] = posts_df['platform'] + "_" + posts_df['platformID']
    
    # Create column commentExportDone if not already present - with False value
    if 'commentExportDone' not in posts_df:
        posts_df['commentExportDone'] = False
    
    # Only do FB and IG posts, which have not been exported previously
    posts_df_fb_ig = posts_df[(posts_df.platform=='facebook') | (posts_df.platform=='instagram')][posts_df.commentExportDone!=True]
    
    # If no posts eligible for comment export: return
    if posts_df_fb_ig.shape[0] == 0:
        print('No eligible FB or IG posts for comment exporting: returning posts_df and empty dataframe comments_df')
        comments_df = pd.DataFrame()
        return posts_df, comments_df
    
    # Create comment exports
    comments_exports = create_comment_exports(posts_df_fb_ig, ex_keys)
    
    # print()
    # print('=====')
    # print()
    
    # Get the export data
    comments_results = get_comment_exports(comments_exports, ex_keys)
    
    # Create a combined df of comments from all posts,
    # including a var postIndex to keep track of which post it's for:
    dfs = []
    for newPostID in comments_results:
        # Convert comments for the post into a df
        comment_df = pd.DataFrame(comments_results[newPostID])

        # Add column for newPostID
        comment_df['newPostID'] = newPostID

        # Move newPostID column to the front
        col = comment_df.pop('newPostID')
        comment_df.insert(0, col.name, col)

        # Add this df to the list of df's
        dfs.append(comment_df)

    # Concatenate all df's together
    if(dfs != []):
        comments_df = pd.concat(dfs)
    else:
        return pd.DataFrame(), pd.DataFrame()

    # To add to posts_df: Column with count of comments for a post
    commentCountExported = comments_df.groupby('newPostID')['commentId'].count().rename('commentCountExported')
    posts_df_comments = posts_df.merge(commentCountExported, left_on='newPostID', right_index=True, how='left')
    posts_df_comments['commentCountExported'] = posts_df_comments.commentCountExported.fillna(0).astype(int)
    posts_df_comments['commentExportDone'] = True
    
    return posts_df_comments, comments_df

# Inputs:

# posts_df: Dataframe with rows of all posts we'd like to pull comments for (this will actually try it for every. single. post. -- so should be limited to FB/IG already)
# ex_keys: Exportcomments API key

# Outputs:

# comments_exports: dict, with keys i = index of post in posts_df; object is r_export.body that contains info about the export created
# (no item in dict created if the post does not have any comments to export - exportcomments gives an error saying so)

def create_comment_exports(posts_df, ex_keys):
    # Instantiate the client Using your API key
    ex = ExportComments(ex_keys['exportcomments']['token'])
    
    print("[exportcomments_fetch] Starting: Create comment export")
    
    # Get start time
    start = time.time()

    # Initiate dict to collect comments export info for each post
    comments_exports = dict()
    
    # Initiate posts-processed counter
    p = 0

    for i, post in posts_df.iterrows():
        # Rate limit for Create the export: "You cannot create more that 20 resources within 3 minutes." = 9 seconds per request
        time.sleep(9)
        
        # Pull newPostID for the post, for indexing
        newPostID = post['newPostID']
        
        # Just for a status update - print for every 50th post's comment fetched
        p = p + 1
        if p % 10 == 0:
            print("[exportcomments_fetch] post #: " + str(p) + ", post index: " + newPostID + ", time: " + str(time.time() - start))        

        # While there is no export made yet - try and create the export:
        export_made = False
        while export_made == False:

            # Create the export:
            r_export = ex.exports.create(
                url=post['url'],
                replies='true' #replies='false'
            )

            # Check API return code for error
            if r_export.body['code'] != 200:
                print('[exportcomments_fetch] post !200 '+ newPostID) # print post index for debugging
                # print(post['url']) # print post URL for debugging
                # print(r_export.body)
                break

            # Check for successful export creation
            if 'guid' in r_export.body['data'].keys():
                export_made = True
                continue

            else:                
                # Check API response for rate-limit error notification:
                if 'error_code' in r_export.body['data'].keys():
#                     print(r_export.body)
                    time.sleep(r_export.body['data']['seconds_to_wait'])
                    continue # try again to make export
                else:
                    print('[exportcomments_fetch] post unsuccessful post '+ newPostID) # print post index for debugging
                    # print(post['url']) # print post URL for debugging
                    # print(r_export.body)
                    break

        # Store the export info
        if export_made == True:
            comments_exports[newPostID] = r_export.body

    # print()
    print("[exportcomments_fetch] posts gone through:"+ str(len(comments_exports)))
    print("[exportcomments_fetch] time taken:"+ str(time.time() - start))
    return comments_exports

# Inputs:

# comments_exports: dict, as outputted by create_comment_exports
# ex_keys: Exportcomments API key

# Outputs:

# comments_results: dict, with keys i = index of post in posts_df; object is resulting json of exported comment

def get_comment_exports(comments_exports, ex_keys):
    # Instantiate the client Using your API key
    ex = ExportComments(ex_keys['exportcomments']['token'])
    
    print("[exportcomments_fetch] Starting: Get comment exports")

    # Get start time
    start = time.time()

    # Initiate dict to collect comments data for each post
    comments_results = dict()
    
    # Initiate posts-processed counter
    p = 0

    for newPostID, r_export in comments_exports.items():
        # Overall rate limit: ExportComments.com limits API usage to 5 requests per second, with any requests thereafter being queued up to a maximum of 5 requests.
        time.sleep(0.2)
        
        # Just for a status update - print for every 50th post's comment fetched
        p = p + 1
        if p % 10 == 0:
            print("[exportcomments_fetch] post #: " + str(p) + ", post index: " + str(i) + ", time: " + str(time.time() - start))

        # Check the export status: "For each run, you may make at most 25 calls during the first 5 minutes after the export started." = 12 seconds per request
        export_status = "not started"
        while export_status != "done":
            r_check = ex.exports.check(
                guid=r_export['data']['guid']
            )
            export_status = r_check.body['data'][0]['status']
            if export_status == "error":
                # Print out post export info for debugging, if it's not a "no comments found" situation
                if not r_check.body['data'][0]['error'].startswith("No comments"):
                    print("[exportcomments_fetch] no comments for " + newPostID) # print post index for debugging
                    # print(r_export['data']['url']) # print post URL for debugging
                    # print(r_check.body)
                break
            if export_status != "done":
                time.sleep(12)

        # Once export status is good...
        if export_status == 'done':
            # Open result json
            r_result = urllib.request.Request('https://www.exportcomments.com' + r_export['data']['rawUrl'], headers={'User-Agent' : "Magic Browser"}) # header is to avoid 403 error...
            result_f = urllib.request.urlopen(r_result)
            result = result_f.read()
            # Add to results dict
            comments_results[newPostID] = json.loads(result)

    print("[exportcomments_fetch] comment exports gone through:" + str(len(comments_exports)) + ", time taken:" + str(time.time() - start))
    return comments_results