from datetime import datetime
from searchtweets import collect_results
from searchtweets import ResultStream, load_credentials, gen_request_parameters


def credential_args(filename="~/.credential.yaml",yaml_key="search_tweets_premium"):
    premium_search_args = load_credentials(filename,
                                       yaml_key=yaml_key,
                                       env_overwrite=False)
    return premium_search_args
def search_rules(q, filters=None, fromdate=None, todate=None, results_per_call=100,tag="covid"):
    #"[query,start_time,end_time,since_id,until_id,max_results,next_token,expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields]"
        query = q
        if filters:
            query = q + ' ' + filters
        rule = gen_request_parameters(query= query, start_time=fromdate, end_time=todate, results_per_call = 10,
                                        user_fields="location,verified",place_fields="place_type,name,country_code",
                                        tweet_fields="id,created_at,text,in_reply_to_user_id,lang") # testing with a sandbox account
        return rule
  
def fetch_results(query_rule,result_stream_args,max_results=100):
    tweets = collect_results(query_rule,
                            max_tweets=max_results,
                            result_stream_args=result_stream_args) # change this if you need to
    return tweets

if __name__=='__main__':
    search_args = credential_args(filename="./credential.yaml",yaml_key="search_tweets_premium")
    #print(search_args)
    rule = search_rules("COVID",filters="lang:en place:Delhi",fromdate="2020-06-09",todate="2020-06-10")
    #print(rule)
    tweets = fetch_results(rule,search_args,20)
    print(len(tweets))
    for tweet in tweets:
        print(tweet)
        # print(tweet['id'],tweet['created_at'],tweet['user']['location'], \
        #     tweet['text'],tweet['lang'],tweet['place']['id'],\
        #     tweet['place']['place_type'],tweet['place']['name'],tweet['place']['country_code'],tweet['user']['verified'],tweet['in_reply_to_status_id'],
        #     tweet['matching_rules'])
        # if tweet.get('extended_tweet',None):
        #     tweet['extended_tweet']['full_text']
        #break