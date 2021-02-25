from datetime import datetime,timedelta 
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
    MAX_TWEETS = 1000
    if START_OVER:
        with open('tweets_response.csv','w') as tweet_f:
            tweet_f.write('tweet_id|created_at|author_id|language|inreply_to|possibly_sensitive|retweet_count|reply_count|like_count|quote_count|search_type|search_city')
        with open('tweets_users.csv','w') as user_f:
            f.write('author_id|id|location|verified')
        with open('tweets_place.csv','w') as place_f:
            user_f.write('place_id|place_type|name|country_code')
    
    search_args = credential_args(filename="./credential.yaml",yaml_key="search_tweets_premium")
    #print(search_args)
    rule = search_rules("COVID",filters="lang:en place:Seattle",fromdate="2019-06-09",todate="2020-06-10")
    #print(rule)
    cities = [['New York',[40.730610,-73.935242,20]],
            ['Seattle',[47.608013,-122.335167,20]]]#'London','Hawaii','Miami','Mumbai','Delhi','Seoul','Singapore','Tokyo','Rome','Auckland','Syndey','Cape Town']
    covid_keywords = ['Coronavirus']
    mental_health_keywords= ['mental health']
    covid_search_term = " OR ".join(covid_keywords)
    mental_search_term = " OR ".join(mental_health_keywords)
    keywords = {'covid_search':covid_search_term,'mental_search': mental_search_term}
    START_DATE = "2019-12-01"
    default_place={'place_id':0}
    for i in range(0,3,1):
        datetime_object = datetime.strptime(START_DATE, '%Y-%m-%d')
        query_start_date = datetime_object+ timedelta(i)
        query_end_date = datetime_object+ timedelta(i+1)
        print(f"Quering for start date {query_start_date} and end date {query_end_date}")
        for query, value in keywords.items():
            for city, radius in cities:
                #point_radius:[longitude latitude radius]
                rule = search_rules(value,filters=f"lang:en -is:retweet has:geo (place:{city} OR point_radius:[{radius[1]} {radius[0]} {radius[2]}mi])"
                                    ,fromdate=query_start_date.strftime('%Y-%m-%d')
                                    ,todate=query_end_date.strftime('%Y-%m-%d'))
                print(f"Query Rule {rule}")
                tweets = fetch_results(rule,search_args,MAX_TWEETS)
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
                    if tweet.get('text'):
                        text = tweet.get('text').replace('\n', ' ').replace('\r', '')
                        with open('tweets_response.csv','a') as tweet_f:
                            tweet_f.write(f"{tweet.get('id')}|{tweet.get('created_at')}|{tweet.get('author_id')}| \
                            {text}|{tweet.get('lang')}|{tweet.get('geo',default_place)['place_id']}|\
                            {tweet.get('in_reply_to_user_id')}| \
                            {tweet.get('possibly_sensitive')}| \
                            {tweet.get('public_metrics',{'retweet_count':0})['retweet_count']}| \
                            {tweet.get('public_metrics',{'reply_count':0})['reply_count']}| \
                            {tweet.get('public_metrics',{'reply_count':0})['like_count']}| \
                            {tweet['public_metrics']['quote_count']}|{query}|{city}\n")


#sample tweets response 
# {'created_at': '2020-06-09T22:58:39.000Z', 
# 'lang': 'en', 'text': 'We did not have access to our Law &amp; Justice Committee for the past several months due to COVID-19, which paused our ability to pass the reform motion. Once the restrictions lifted last week, we felt it was important to provide justice to the family and quickly request action. 3/6', 
# 'in_reply_to_user_id': '1176880543', 
# 'geo': {'place_id': '300bcc6e23a88361'}, 
# 'public_metrics': {'retweet_count': 0, 'reply_count': 1, 'like_count': 6, 'quote_count': 0}, 
# 'id': '1270490520284884992', 'possibly_sensitive': False, 'author_id': '1176880543'}