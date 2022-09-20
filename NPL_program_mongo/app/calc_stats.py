import json
import re
from pymongo import MongoClient
from bson.son import SON
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import numpy as np
from PIL import Image
from bson.objectid import ObjectId
from bson.dbref import DBRef


def initialize_db():
    clientConfigServer = MongoClient('configs1', 27017, directConnection=True)
    config = {'_id': 'cfgrs', 'members': [
        {'_id': 0, 'host': 'configs1:27017'}
    ]}
    try:
        clientConfigServer.admin.command("replSetInitiate", config)
    except:
        pass

    clientShard = MongoClient('shard1s1', 27017, directConnection=True)
    configShard = {'_id': 'shard1rs', 'members': [
        {'_id': 0, 'host': 'shard1s1:27017'},
        {'_id': 1, 'host': 'shard1s2:27017'}
    ]}
    try:
        clientShard.admin.command("replSetInitiate", configShard)
    except:
        pass

    client = MongoClient('mongos', 27017, directConnection=True)
    db = client.admin
    try:
        db.command("addShard", "shard1rs/shard1s1:27017,shard1s2:27017")
        db.command("enableSharding", "nosqldb")
        db.command({"shardCollection": "nosqldb.Twitter", 'key': {'_id': 'hashed'}})
        db.command({"shardCollection": "nosqldb.LexResourcesWords", 'key': {'_id': 'hashed'}})
        db.command({"shardCollection": "nosqldb.LexResources", 'key': {'_id': 'hashed'}})
        db.command({"shardCollection": "nosqldb.countWords", 'key': {'_id': 'hashed'}})
        db.command({"shardCollection": "nosqldb.countHashtags", 'key': {'_id': 'hashed'}})
        db.command({"shardCollection": "nosqldb.countEmojis", 'key': {'_id': 'hashed'}})
        db.command({"shardCollection": "nosqldb.countEmoticons", 'key': {'_id': 'hashed'}})
    except:
        pass


def insert_twitter():
    with open("/code/dict_twitter_complete.txt", 'r', encoding="utf8") as input_file:
        list_twit = json.load(input_file)
    client = MongoClient('mongos', 27017)
    db = client["nosqldb"]
    collection_twitter = db["Twitter"]
    result = collection_twitter.insert_many(list_twit)


def insert_lexwords():
    with open("/code/dict_lex_resources_words.txt", 'r', encoding="utf8") as input_file:
        list_lexwords = json.load(input_file)
    client = MongoClient('mongos', 27017)
    db = client["nosqldb"]
    collection_lexwords = db["LexResourcesWords"]
    result = collection_lexwords.insert_many(list_lexwords)


def insert_lex():
    with open("/code/dict_lex_resources.txt", 'r', encoding="utf8") as input_file:
        list_lex = json.load(input_file)
    client = MongoClient('mongos', 27017)
    db = client["nosqldb"]
    collection_lex = db["LexResources"]
    result = collection_lex.insert_many(list_lex)
    print(result.inserted_ids)


def my_map_reduce():
    client = MongoClient('mongos', 27017)
    db = client["nosqldb"]
    collection = db["Twitter"]
    pipeline_words = [
        {"$unwind": "$words"},
        {"$group": {"_id": {"word": "$words.lemma", "POS": "$words.POS"}, "count": {"$sum": "$words.freq"}}},
        {"$sort": SON([("count", -1), ("_id", -1)])}
    ]

    pipeline_hashtag = [
        {"$unwind": "$hashtags"},
        {"$group": {"_id": "$hashtags", "count": {"$sum": 1}}},
        {"$sort": SON([("count", -1), ("_id", -1)])}
    ]

    pipeline_emoji = [
        {"$unwind": "$emojis"},
        {"$group": {"_id": "$emojis", "count": {"$sum": 1}}},
        {"$sort": SON([("count", -1), ("_id", -1)])}
    ]

    pipeline_emoticon = [
        {"$unwind": "$emoticons"},
        {"$group": {"_id": "$emoticons", "count": {"$sum": 1}}},
        {"$sort": SON([("count", -1), ("_id", -1)])}
    ]

    my_resultWords = list(collection.aggregate(pipeline_words))
    collection_result = db["countWords"]
    result = collection_result.insert_many(my_resultWords)
    print(result.inserted_ids)

    my_resultHashtags = list(collection.aggregate(pipeline_hashtag))
    collection_result = db["countHashtags"]
    result = collection_result.insert_many(my_resultHashtags)
    print(result.inserted_ids)

    my_resultEmojis = list(collection.aggregate(pipeline_emoji))
    collection_result = db["countEmojis"]
    result = collection_result.insert_many(my_resultEmojis)
    print(result.inserted_ids)

    my_resultEmoticons = list(collection.aggregate(pipeline_emoticon))
    collection_result = db["countEmoticons"]
    result = collection_result.insert_many(my_resultEmoticons)
    print(result.inserted_ids)

    my_result = [*my_resultWords, *my_resultHashtags, *my_resultEmojis, *my_resultEmoticons]

    with open("/code/result_map_reduce.txt", 'w', encoding="utf8") as output_file:
        json.dump(my_result, output_file)


# calcolare le statistiche
def calculate_statistics():
    client = MongoClient('mongos', 27017)
    db = client["nosqldb"]
    collectionLexResourcesWords = db["LexResourcesWords"]
    collectionTwitter = db["Twitter"]
    collectionWords = db["countWords"]
    collectionLexResources = db["LexResources"]
    sentiment_list = ["anger", "anticipation", "disgust",
                      "fear", "joy", "sadness", "trust", "surprise", ]
    dict_result = {}
    dict_sentiment = {}
    for sentiment in sentiment_list:
        pipeline = [
            {"$match": {"sentiment": sentiment}},
            {"$unwind": "$words"},
            {"$group": {"_id": sentiment, "count": {"$sum": "$words.freq"}}}
        ]
        words_sentiment = list(collectionTwitter.aggregate(pipeline))
        max_count = words_sentiment[0]['count']  # N_twitter_words(Y)
        list_currentResources = list(collectionLexResources.find({'sentiment': sentiment}))
        for documentResource in list_currentResources:
            resourceNumWords = documentResource['totNumberWords']  # N_lex_words(X)
            list_cursor = list(
                collectionLexResourcesWords.find({'resources.$id': documentResource['_id']}, {'lemma': 1}))
            dict_sentiment[documentResource['_id']] = []
            for word in list_cursor:
                list_cursor2 = list(collectionWords.find({'_id.word': word['lemma']}))
                if len(list_cursor2) > 0:
                    dict_sentiment[documentResource['_id']].append(word['lemma'])
            # calcolo le statistiche
            result1 = "perc_presence_lex_res(" + documentResource['_id'] + "," + sentiment + ")"
            result2 = "perc_presence_twitter(" + documentResource['_id'] + "," + sentiment + ")"
            dict_result[result1] = (len(dict_sentiment[documentResource['_id']]) / resourceNumWords) * 100
            dict_result[result2] = (len(dict_sentiment[documentResource['_id']]) / max_count) * 100
            dict_result[result1] = f'{dict_result[result1]:.3f}'[:-1]
            dict_result[result2] = f'{dict_result[result2]:.3f}'[:-1]
            dict_result[result1] = str(dict_result[result1]) + " %"
            dict_result[result2] = str(dict_result[result2]) + " %"
    with open("./result_statistics.txt", 'w', encoding="utf8") as output_file:
        json.dump(dict_result, output_file)


# creare una word cloud per ogni sentimento, per emoji, hashtag ed emoticon
def word_clouds():
    client = MongoClient('mongos', 27017)
    db = client["nosqldb"]
    collection = db["countWords"]
    cursor = collection.find({})
    # [{'_id': 'word', 'POS': 'pos', count: 203}, ..]
    mask = np.array(Image.open(r'./Twitter_bird_logo.png'))
    dictionary_pos = {}
    for document in cursor:
        if str(document['_id']['word']).isalpha():
            dictionary_pos[(document['_id']['word'], document['_id']['POS'])] = document['count']
    dictionary = {}
    for _ in range(0, 100):
        max_value = 0
        max_key = ''
        for key, value in dictionary_pos.items():
            if max_value < value:
                max_value = value
                max_key = key
        dictionary[max_key[0]] = max_value
        dictionary_pos.pop(max_key)

    wc = WordCloud(mask=mask, background_color="white", max_font_size=500, width=mask.shape[1], height=mask.shape[0],
                   random_state=42, max_words=100, relative_scaling=0.5,
                   normalize_plurals=False).generate_from_frequencies(dictionary)
    plt.figure(figsize=(20, 10))
    plt.imshow(wc)
    plt.axis('off')
    plt.savefig('./word_cloud_words.png', dpi=100)

    collection = db["countHashtags"]
    cursor = collection.find({})
    dictionary.clear()
    for document in cursor:
        dictionary[document['_id']] = document['count']
    wc = WordCloud(mask=mask, background_color="white", max_font_size=500, width=mask.shape[1], height=mask.shape[0],
                   random_state=42, max_words=100, relative_scaling=0.5,
                   normalize_plurals=False).generate_from_frequencies(dictionary)
    plt.figure(figsize=(20, 10))
    plt.imshow(wc)
    plt.axis('off')
    plt.savefig('./word_cloud_hashtags.png', dpi=100)

    collection = db["countEmojis"]
    cursor = collection.find({})
    dictionary.clear()
    for document in cursor:
        dictionary[document['_id']] = document['count']
    wc = WordCloud(font_path='./NotoEmoji.ttf', mask=mask, background_color="white", max_font_size=500,
                   width=mask.shape[1], height=mask.shape[0], random_state=42, max_words=100, relative_scaling=0.5,
                   normalize_plurals=False).generate_from_frequencies(dictionary)
    plt.figure(figsize=(20, 10))
    plt.imshow(wc)
    plt.axis('off')
    plt.savefig('./word_cloud_emojis.png', dpi=100)

    collection = db["countEmoticons"]
    cursor = collection.find({})
    dictionary.clear()
    for document in cursor:
        dictionary[document['_id']] = document['count']
    wc = WordCloud(mask=mask, background_color="white", max_font_size=500, width=mask.shape[1], height=mask.shape[0],
                   random_state=42, max_words=100, relative_scaling=0.5,
                   normalize_plurals=False).generate_from_frequencies(dictionary)
    plt.figure(figsize=(20, 10))
    plt.imshow(wc)
    plt.axis('off')
    plt.savefig('./word_cloud_emoticons.png', dpi=100)


# creare un istogramma per ogni sentimento con le percentuali delle parole per ogni sentimento
def calculate_histogram():
    client = MongoClient('mongos', 27017)
    db = client["nosqldb"]
    collectionWords = db["countWords"]
    sentiment_list = ["anger", "anticipation", "disgust",
                      "fear", "joy", "sadness", "trust", "surprise", ]
    collectionLex = db["LexResourcesWords"]
    collectionLex2 = db["LexResources"]
    collectionTwitter = db["Twitter"]
    dictionary = {}
    for sentiment in sentiment_list:
        pipeline = [
            {"$match": {"sentiment": sentiment}},
            {"$unwind": "$words"},
            {"$group": {"_id": sentiment, "count": {"$sum": "$words.freq"}}}
        ]
        words_sentiment = list(collectionTwitter.aggregate(pipeline))
        max_count = words_sentiment[0]['count']
        cursor = collectionWords.find({}).sort('count', -1)
        for document in cursor:
            cursor2 = list(collectionLex.find({'lemma': document['_id']['word']}))
            if len(cursor2) > 0:
                list_resources = cursor2[0]['resources']
                for resource in list_resources:
                    cursor3 = list(collectionLex2.find({'_id': resource.as_doc().get('$id'), 'sentiment': sentiment}))
                    if len(cursor3) > 0:
                        dictionary[document['_id']['word']] = document['count'] / max_count
        if sentiment == "joy":
            print("max_count = " + str(max_count))
            z = 0
            for v in dictionary.values():
                z += v
            print("Numero di parole totale: " + str(z))
            print(dictionary)

        sum_v = 0
        for value in dictionary.values():
            sum_v += value
        sum_v = sum_v * 100
        for key, value in dictionary.items():
            dictionary[key] = value * 100
        dictionary_result = {}
        for _ in range(0, 20):
            max_v = 0
            key_max = ''
            for key, value in dictionary.items():
                if max_v < value:
                    max_v = value
                    key_max = key
            dictionary_result[key_max] = max_v
            dictionary.pop(key_max)

        partial_sum = 0
        for value in dictionary.values():
            partial_sum += value
        dictionary_result['else'] = partial_sum
        plt.bar(list(map(str, dictionary_result.keys())), list(map(float, dictionary_result.values())))
        plt.xticks(rotation='vertical')
        plt.title(sentiment + ' bar plot, (tot perc = ' + str(f'{sum_v:.3f}'[:-1]) + '%)')
        plt.xlabel('Words')
        plt.ylabel('Percentage')
        plt.show()
        plt.savefig('./bar_' + sentiment + '.png', dpi=100)
        plt.clf()
        dictionary.clear()


# raccogliere le parole nuove presenti nei tweet e non nelle risorse lessicali
def new_words():
    client = MongoClient('mongos', 27017)
    db = client["nosqldb"]
    collectionTwitter = db["Twitter"]
    pipeline = [
        {"$unwind": "$words"},
        {"$match": {"words.in_lex_resources": {}}},
        {"$group": {"_id": {"word":"$words.lemma", "POS": "$words.POS"}}}
    ]
    list_words = list(collectionTwitter.aggregate(pipeline))
    with open("/code/new_words.txt", 'w', encoding="utf8") as output_file:
        json.dump(list_words, output_file)


def main():
    #initialize_db()
    #insert_lex()
    #insert_lexwords()
    #insert_twitter()
    #my_map_reduce()
    #word_clouds()
    #new_words()
    #calculate_statistics()
    calculate_histogram()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
