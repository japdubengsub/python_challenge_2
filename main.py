#!/usr/bin/env python3

"""
I tried to keep this script simple and do not require any library to run
"""

import argparse
import json
import logging
import sqlite3
import urllib.request
from pathlib import Path
from typing import Tuple

URL_POSTS = "https://jsonplaceholder.typicode.com/posts"
URL_COMMENTS = "https://jsonplaceholder.typicode.com/comments"


def get_url(url: str) -> dict:
    """
    get json data by url
    """
    try:
        with urllib.request.urlopen(url) as response:
            html = response.read()
    except (ValueError, AttributeError) as e:
        logging.error(e)
        return {}

    try:
        data = json.loads(html)
    except (TypeError, json.JSONDecodeError) as e:
        logging.error(e)
        return {}

    return data


def parse_args() -> Tuple[str, str]:
    """
    parse command-line params
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--posts', help='url for posts')
    parser.add_argument('--comments', help='url for comments')
    args = parser.parse_args()

    url_posts = args.posts or URL_POSTS
    url_comments = args.comments or URL_COMMENTS

    return url_posts, url_comments


def init_db() -> sqlite3.Connection:
    create_tables = True
    if Path("data.db").is_file():
        create_tables = False

    con = sqlite3.connect('data.db')
    cur = con.cursor()

    if create_tables:
        cur.execute(
            '''CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id int, title text, body text)''')
        cur.execute(
            '''CREATE TABLE comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id int,
                name text,
                email text,
                body text,
                FOREIGN KEY(post_id) REFERENCES posts (id)
                )''')
        con.commit()

    return con


def print_all(db):
    cur = db.cursor()

    cur.execute(
        """
        SELECT posts.id, posts.user_id, posts.title, posts.body,
                comments.id, comments.post_id, comments.name, comments.email, comments.body
        FROM posts 
        LEFT JOIN comments
            ON comments.post_id = posts.id
        ORDER BY posts.id, comments.id
        """
    )

    for row in cur:
        print(row)


def save_posts(db, posts):
    posts = get_url(posts)

    for post in posts:
        try:
            db.execute("""INSERT INTO posts VALUES ({}, {}, '{}', '{}')""".format(
                post['id'], post['userId'], post['title'], post['body']))
        except sqlite3.IntegrityError:
            logging.warning("record already exists: {}".format(post))
        except KeyError as e:
            logging.error(e)


def save_comments(db, comments):
    comments = get_url(comments)

    for comment in comments:
        try:
            db.execute("""INSERT INTO comments VALUES ({}, {}, '{}', '{}', '{}')""".format(
                comment['id'], comment['postId'], comment['name'], comment['email'], comment['body']))
        except sqlite3.IntegrityError:
            logging.warning("record already exists: {}".format(comment))
        except KeyError as e:
            logging.error(e)

    db.commit()


if __name__ == '__main__':
    db_conn = init_db()

    posts_url, comments_url = parse_args()

    save_posts(db_conn, posts_url)
    save_comments(db_conn, comments_url)

    print_all(db_conn)

    db_conn.close()
