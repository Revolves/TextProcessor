import tweepy

# import argparse
# API setting
language = "zh"
url = 'www.twitter.com'
page_type = 'twitter'
site_name = 'twitter'


def get_api(consumer_key, consumer_secret, access_key, access_secret):
    """获取API"""
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)
    return api


def get_twitter_url(user_name, status_id):
    "生成该条tweet的链接"
    return 'https://' + url + '/' + str(user_name) + "/status/" + str(status_id)


def get_title(content):
    "获取正文标题"
    return str(content)[0:30]


def get_publishTime(Time):
    "生成无符号时间"
    return Time.strftime("%Y%m%d%H%M%S")


def dataget(api, keyword):
    result = []
    count = 0
    for Status in tweepy.Cursor(api.search, keyword, tweet_mode='extended', show_user=True).items():
        # print(Status)
        item = {'keyword': keyword, 'date': get_publishTime(Status.created_at),
                'url': get_twitter_url(Status.author.screen_name, Status.id)}
        count += 1
        # retweet
        try:
            if str(Status.full_text)[0:2] == 'RT':
                item['content'] = Status.retweeted_status.full_text.replace('\n', ' ')
            # elif Status.in_reply_to_status_id is not None:
            #     item['content'] = Status.full_text.replace('\n', ' ')
            else:
                item['content'] = Status.full_text.replace('\n', ' ')
        except:
            item['content'] = Status.full_text.replace('\n', ' ')
        item['source'] = 'Twitter'
        item['title'] = get_title(item['content'])
        result.append(item)
        if count > 20:
            break
    return result

