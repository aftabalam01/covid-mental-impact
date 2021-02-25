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
        rule = gen_request_parameters(query= query, start_time=fromdate, end_time=todate, results_per_call = 10,
                                        user_fields="location,verified",place_fields="place_type,name,country_code",
                                        tweet_fields="id,created_at,text,in_reply_to_user_id,lang,geo,author_id,possibly_sensitive,public_metrics") 
        #{"tweet.fields":[attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,non_public_metrics,organic_metrics,possibly_sensitive,promoted_metrics,public_metrics,referenced_tweets,reply_settings,source,text,withheld]"}]
        return rule
  
def fetch_results(query_rule,result_stream_args,max_results=100):
    tweets = collect_results(query_rule,
                            max_tweets=max_results,
                            result_stream_args=result_stream_args) # change this if you need to
    return tweets

if __name__=='__main__':
    search_args = credential_args(filename="./credential.yaml",yaml_key="search_tweets_premium")
    #print(search_args)
    rule = search_rules("COVID",filters="lang:en place:Seattle",fromdate="2020-06-09",todate="2020-06-10")
    #print(rule)
    tweets = fetch_results(rule,search_args,1000)
    print(len(tweets))
    for tweet in tweets:
        print(tweet)
        print(tweet['id'],tweet['created_at'],tweet['author_id'], \
            tweet['text'],tweet['lang'],tweet['geo']['place_id'],\
            tweet.get('in_reply_to_user_id'), \
            tweet['possibly_sensitive']), \
            tweet['public_metrics']['retweet_count'], \
            tweet['public_metrics']['reply_count'], \
            tweet['public_metrics']['like_count'], \
            tweet['public_metrics']['quote_count']
        break
#sample tweets response 
# {'created_at': '2020-06-09T22:58:39.000Z', 
# 'lang': 'en', 'text': 'We did not have access to our Law &amp; Justice Committee for the past several months due to COVID-19, which paused our ability to pass the reform motion. Once the restrictions lifted last week, we felt it was important to provide justice to the family and quickly request action. 3/6', 
# 'in_reply_to_user_id': '1176880543', 
# 'geo': {'place_id': '300bcc6e23a88361'}, 
# 'public_metrics': {'retweet_count': 0, 'reply_count': 1, 'like_count': 6, 'quote_count': 0}, 
# 'id': '1270490520284884992', 'possibly_sensitive': False, 'author_id': '1176880543'}