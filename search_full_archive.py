from datetime import datetime,timedelta 
from searchtweets import collect_results
from searchtweets import ResultStream, load_credentials, gen_request_parameters
import requests
import json


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
        rule = gen_request_parameters(query= query, start_time=fromdate, end_time=todate, expansions="geo.place_id,author_id",
                                        results_per_call = results_per_call,
                                        user_fields="id,location,verified",place_fields="place_type,name,country_code",
                                        tweet_fields="id,created_at,text,in_reply_to_user_id,lang,geo,author_id,possibly_sensitive,public_metrics") 
        #{"tweet.fields":[attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,non_public_metrics,organic_metrics,possibly_sensitive,promoted_metrics,public_metrics,referenced_tweets,reply_settings,source,text,withheld]"}]
        return rule
  
def fetch_results(query_rule,result_stream_args,max_results=100):
    tweets = collect_results(query_rule,
                            max_tweets=max_results,
                            result_stream_args=result_stream_args) # change this if you need to
    return tweets

def search_geocity(query,headers):
    endpoint='https://api.twitter.com/1.1/geo/search.json'
    params = {'query':query,'granularity':'neighborhood','trim_place':'true'}
    res = requests.get(url=endpoint, params=params, headers=headers)
    print(res.text)

if __name__=='__main__':
    START_OVER = False
    MAX_TWEETS = 10000
    if START_OVER:
        with open('tweets_response.csv','w') as tweet_f:
            tweet_f.write('tweet_id|created_at|author_id|language|inreply_to|possibly_sensitive|retweet_count|reply_count|like_count|quote_count|search_type|search_city')
        with open('tweets_users.csv','w') as user_f:
            f.write('author_id|id|location|verified')
        with open('tweets_place.csv','w') as place_f:
            user_f.write('place_id|place_type|name|country_code')
    
    search_args = credential_args(filename="./credential.yaml",yaml_key="search_tweets_premium")
    #search_geocity('Seattle',{'Authorization':f"Bearer {search_args['bearer_token']}"})
    #rule = search_rules("COVID",filters="lang:en place:Seattle",fromdate="2019-06-09",todate="2020-06-10")
    #print(rule)
    # cities = [['New York',[40.730610,-73.935242,25]],['Seattle',[47.608013,-122.335167,25]],
    #           ['Hawaii',[19.741755,-155.844437,25]],['Miami',[25.761681,-80.191788,25]],['London',[51.509865,-0.118092,25]]
    #       ,['Mumbai',[19.076090,72.877426,25]], ['Delhi',[28.644800,77.216721,25]],['Seoul',[37.532600,127.024612,25]]
    #       ,['Singapore',[1.290270,103.851959,25]], ['Tokyo',[35.652832,139.839478,25]]
    #       ,['Rome',[41.902782,12.496366,25]],['Auckland',[-36.848461,174.763336,25]],['Syndey',[-33.865143,151.209900,25]],
    #       ['Cape Town',[-33.918861,18.423300,25]],['Manaus-Brazil',[-3.117034,-60.025780,25]]
    #       ]

    covid_search_term = "Asymptomatic OR Coronavirus OR (Community spread) OR Ventilator OR PPE (Social distancing) OR (Self isolation) OR (Self quarantine) OR (Shelter in place) OR mask OR N95 OR (Herd immunity) OR Vaccine OR COVID"
    mental_search_term = "(mental health) OR depression OR stress OR addiction OR alcoholism OR anxiety OR (health anxiety) OR lonely OR ptsd OR schizophrenia OR (social anxiety) OR suicide OR meditation OR therapy OR counsel OR emotion OR crazy"
    keywords = {'covid_search':covid_search_term,'mental_search': mental_search_term}
    dates = ["2020-01-01","2020-04-01","2020-07-01","2020-10-01","2021-01-01"]
    error_tweets= {}
    error_id = 0
    for START_DATE in dates:
        default_place={'place_id':0}
        for i in range(0,31,1):
            datetime_object = datetime.strptime(START_DATE, '%Y-%m-%d')
            query_start_date = datetime_object+ timedelta(i)
            query_end_date = datetime_object+ timedelta(i+1)
            print(f"Quering for start date {query_start_date} and end date {query_end_date}")
            for query, value in keywords.items():
               # for city, radius in cities:
                #point_radius:[longitude latitude radius]
                rule = search_rules(value,filters=f"lang:en -is:retweet -is:reply" #has:geo (place:{city} OR point_radius:[{radius[1]} {radius[0]} {radius[2]}mi])"
                                    ,fromdate=query_start_date.strftime('%Y-%m-%d')
                                    ,todate=query_end_date.strftime('%Y-%m-%d'))
                #print(f"Query Rule {rule}")
                tweets = fetch_results(rule,search_args,MAX_TWEETS)
                for tweet in tweets:
                    try:
                        # if 'users' in tweet.keys():
                        #     for user in tweet['users']:
                        #         with open('tweets_users.csv','a') as user_f:
                        #             user_f.write(f"{user.get('author_id')}|{user.get('id')}|{user.get('location')}|{user.get('verified')}\n")
                        #     continue

                        # if 'places' in tweet.keys():
                        #     for place in tweet['places']:
                        #         with open('tweets_place.csv','a') as place_f:
                        #             place_f.write(f"{place.get('id')}|{place.get('place_type')}|{place.get('name')}|{place.get('country_code')}\n")
                        #     continue
                        if tweet.get('text'):
                            text = tweet.get('text').replace('\n', ' ').replace('\r', '')
                            with open('tweets_response.csv','a') as tweet_f:
                                tweet_f.write(f"{tweet.get('id')}|{tweet.get('created_at')}|{tweet.get('author_id')}| \
                                {text}|{tweet.get('lang')}|{tweet.get('geo',default_place)['place_id']}|\
                                {tweet.get('in_reply_to_user_id')}| \
                                {tweet.get('possibly_sensitive')}| \
                                {tweet.get('public_metrics',{'retweet_count':0})['retweet_count']}| \
                                {tweet.get('public_metrics',{'reply_count':0})['reply_count']}| \
                                {tweet.get('public_metrics',{'like_count':0})['like_count']}| \
                                {tweet['public_metrics']['quote_count']}|{query}\n")
                    except: 
                        error_tweets.update({error_id:tweet})
    with open('error_tweets.json','w') as f :
        f.write(json.dumps(error_tweets))

#sample tweets response 
# {'created_at': '2020-06-09T22:58:39.000Z', 
# 'lang': 'en', 'text': 'We did not have access to our Law &amp; Justice Committee for the past several months due to COVID-19, which paused our ability to pass the reform motion. Once the restrictions lifted last week, we felt it was important to provide justice to the family and quickly request action. 3/6', 
# 'in_reply_to_user_id': '1176880543', 
# 'geo': {'place_id': '300bcc6e23a88361'}, 
# 'public_metrics': {'retweet_count': 0, 'reply_count': 1, 'like_count': 6, 'quote_count': 0}, 
# 'id': '1270490520284884992', 'possibly_sensitive': False, 'author_id': '1176880543'}