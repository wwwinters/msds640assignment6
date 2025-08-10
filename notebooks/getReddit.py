#!/usr/bin/env python

#######################################
# Author:   Wiley Winters
# Name:     getReddit.py
# Purpose:  Download Reddit submitions
#           and posts
# Date:     2023-09-27
#######################################

import praw
import credentials
import sqlite3
import pandas as pd 
from tqdm import tqdm
import argparse

#
# Create parser object and read
# command line arguments
#
parser = argparse.ArgumentParser()
parser.add_argument('--name', default='poverty',
                    help='Name of the subreddit to download')

args = parser.parse_args()

reddit_name = args.name

data_file = 'data/'+reddit_name+'.sqlite'

#
# make connection to Reddit API
#
def connect():
    reddit = praw.Reddit(ratelimit_seconds=1000,
                         client_id=credentials.client_id,
                         client_secret=credentials.client_secret,
                         user_agent=credentials.user_agent)
   
    return reddit

#
# build post object
#
def get_posts(reddit):
   
   print('Retrieving posts:')
   
   # define post data structure
   post_data = {'id': [],
                'created_utc': [],
                'title': [],
                'link': [],
                'author': [],
                'n_comments': [],
                'score': [],
                'ratio': [],
                'text': []}

   subreddit = reddit.subreddit(reddit_name).top(limit=None)
   for post in tqdm(list(subreddit)):
      post_data['id'].append(post.id)
      post_data['created_utc'].append(post.created_utc)
      post_data['title'].append(post.title)
      post_data['link'].append(post.permalink)
      post_data['author'].append(post.author)
      post_data['n_comments'].append(post.num_comments)
      post_data['score'].append(post.score)
      post_data['ratio'].append(post.upvote_ratio)
      post_data['text'].append(post.selftext)
   
   post_df = pd.DataFrame(post_data, index=None)
   return(post_df)

#
# build comment object
#
def get_comments(reddit, post_df):
   
   print('Retrieving comments:')

   # define comment data structure
   comment_data = {'comment_id': [],
                   'link_id': [],
                   'comment_utc': [],
                   'comment_author': [],
                   'body': [],
                   'comment_score': []}
   
   for row in tqdm(post_df.id):
      submission = reddit.submission(row)
      submission.comments.replace_more(limit=0)
      for comment in submission.comments.list():
         comment_data['comment_id'].append(comment.id)
         # split the submission ID from the prefix
         _, link_id = comment.link_id.split('_')
         # The comment author was giving me a interface error
         # Changing the object type to string fixed the issue
         author = str(comment.author)
         comment_data['comment_author'].append(author)
         comment_data['link_id'].append(link_id)
         comment_data['comment_utc'].append(comment.created_utc)
         comment_data['body'].append(comment.body)
         comment_data['comment_score'].append(comment.score)

   comment_df = pd.DataFrame(comment_data, index=None)
   return(comment_df)

#
# write dataframes to sqlite db
#
def write_df(post_df, comment_df):
    con = sqlite3.connect(data_file)
    # The author column has mixed datatypes which confuses sqlite
    post_df['author'] = post_df['author'].astype('str')
    post_df.to_sql('posts', con, if_exists='replace', index=False)
    comment_df.to_sql('comments', con, if_exists='replace', index=False)
    con.close()

def main():
    reddit = connect()
    post_df = get_posts(reddit)
    comment_df = get_comments(reddit, post_df)
    write_df(post_df, comment_df)

if __name__ == '__main__':
    main()