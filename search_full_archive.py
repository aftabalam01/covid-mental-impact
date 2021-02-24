
from searchtweets import collect_results
from searchtweets import ResultStream, gen_rule_payload, load_credentials


def credential_args(filename="~/.credential.yaml",yaml_key="search_tweets_premium"):
    premium_search_args = load_credentials(filename,
                                       yaml_key=yaml_key,
                                       env_overwrite=False)
    return premium_search_args
def search_rules(q, filters=None, fromdate=None, todate=None, results_per_call=100,tag="covid"):
        query = q
        if filters:
            query = q + ' ' + filters
        rule = gen_rule_payload(query, from_date=fromdate, to_date = todate, results_per_call=100,tag=tag) # testing with a sandbox account
        return rule
  
def fetch_results(query_rule,result_stream_args,max_results=100):
    tweets = collect_results(query_rule,
                            max_results=max_results,
                            result_stream_args=result_stream_args) # change this if you need to
    return tweets

if __name__=='__main__':
    search_args = credential_args(filename="./credential.yaml",yaml_key="search_tweets_premium")
    print(search_args)
    rule = search_rules("COVID",filters="lang:en place:Seattle",fromdate="2019-12-01",todate="2020-09-02")
    print(rule)
    tweets = fetch_results(rule,search_args,10)
    for tweet in tweets:
        print(tweet['id'],tweet['created_at'],tweet['user']['location'], \
            tweet['text'],tweet['lang'],tweet['place']['id'],\
            tweet['place']['place_type'],tweet['place']['name'],tweet['place']['country_code'],tweet['user']['verified'],tweet['in_reply_to_status_id'],
            tweet['matching_rules'])
        if tweet.get('extended_tweet',None):
            tweet['extended_tweet']['full_text']
        break