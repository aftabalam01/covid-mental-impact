from datetime import datetime
from searchtweets import collect_results
from searchtweets import ResultStream, load_credentials, gen_request_parameters


def credential_args(filename="~/.credential.yaml",yaml_key="search_tweets_premium"):
    premium_search_args = load_credentials(filename,
                                       yaml_key=yaml_key,
                                       env_overwrite=False)
    return premium_search_args

def search_rules(q, filters=None, fromdate=None, todate=None, results_per_call=500,tag="covid"):
    #"[query,start_time,end_time,since_id,until_id,max_results,next_token,expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields]"
        query = q
        if filters:
            query = q + ' ' + filters
        rule = gen_request_parameters(query= query, start_time=fromdate, end_time=todate, results_per_call = results_per_call,
                                        user_fields="id,location,verified",place_fields="place_type,name,country_code",
                                        tweet_fields="id,created_at,text,in_reply_to_user_id,lang,geo,author_id,possibly_sensitive,public_metrics") 
        #{"tweet.fields":[attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,non_public_metrics,organic_metrics,possibly_sensitive,promoted_metrics,public_metrics,referenced_tweets,reply_settings,source,text,withheld]"}]
        return rule
  
def fetch_results(query_rule,result_stream_args,max_results=100):
    tweets = collect_results(query_rule,
                            max_tweets=max_results,
                            result_stream_args=result_stream_args) # change this if you need to
    return tweets

if __name__=='__main__':
    START_OVER = False
    search_args = credential_args(filename="./credential.yaml",yaml_key="search_tweets_premium")
    #print(search_args)
    rule = search_rules("COVID",filters="lang:en place:Seattle",fromdate="2019-06-09",todate="2020-06-10")
    #print(rule)
    tweets = fetch_results(rule,search_args,10)
    if START_OVER:
        with open('tweets_response.csv','w') as tweet_f:
            tweet_f.write('tweet_id|created_at|author_id|language|inreply_to|possibly_sensitive|retweet_count|reply_count|like_count|quote_count')
        with open('tweets_users.csv','w') as user_f:
            f.write('author_id|id|location|verified')
        with open('tweets_place.csv','w') as place_f:
            user_f.write('place_id|place_type|name|country_code')
    
    for tweet in tweets:
        if 'users' in tweet.keys():
            for user in tweet['users']:
                with open('tweets_place.csv','a') as place_f:
                    user_f.write(f"{user.get('author_id')}|{user.get('id')}|{user.get('location')}|{user.get('verified')}\n")
            continue

        if 'places' in tweet.keys():
            for place in tweet['places']:
                with open('tweets_users.csv','a') as user_f:
                    place_f.write(f"{place.get('place_id')}|{place.get('place_type')}|{place.get('name')}|{place.get('country_code')}\n")
            continue
        text = tweet['text'].replace('\n', ' ').replace('\r', '')
        with open('tweets_response.csv','a') as tweet_f:
            tweet_f.write(f"{tweet['id']}|{tweet['created_at']}|{tweet['author_id']}| \
            {text}|{tweet['lang']}|{tweet['geo']['place_id']}|\
            {tweet.get('in_reply_to_user_id')}| \
            {tweet['possibly_sensitive']}| \
            {tweet['public_metrics']['retweet_count']}| \
            {tweet['public_metrics']['reply_count']}| \
            {tweet['public_metrics']['like_count']}| \
            {tweet['public_metrics']['quote_count']}\n")
        break


#sample tweets response 
# {'created_at': '2020-06-09T22:58:39.000Z', 
# 'lang': 'en', 'text': 'We did not have access to our Law &amp; Justice Committee for the past several months due to COVID-19, which paused our ability to pass the reform motion. Once the restrictions lifted last week, we felt it was important to provide justice to the family and quickly request action. 3/6', 
# 'in_reply_to_user_id': '1176880543', 
# 'geo': {'place_id': '300bcc6e23a88361'}, 
# 'public_metrics': {'retweet_count': 0, 'reply_count': 1, 'like_count': 6, 'quote_count': 0}, 
# 'id': '1270490520284884992', 'possibly_sensitive': False, 'author_id': '1176880543'}