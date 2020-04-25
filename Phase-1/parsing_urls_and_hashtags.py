import json

'''
Give the path of the collected file preferable JSON file in place of tweets_1.json
'''
with open('tweets_1.json', 'rt') as tweets_file:
    for i in tweets_file:
        try:
            data_load = json.loads(i)
            for i in data_load['entities']['hashtags']:
                with open("hashtags.txt", "a+")as hashtags_collection:
                    print i['text']
                    hashtags_collection.write(i['text'] + "\n")
            for i in data_load['entities']['urls']:
                with open("urls.txt", "a+") as url_collections:
                    print i['url']
                    url_collections.write(i['url'] + "\n")
                    print i['url']
        except Exception as e:
            print e
