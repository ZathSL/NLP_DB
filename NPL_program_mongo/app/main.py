import ast
import json
import os
import re

directory_resource_host = "../resource/"
directory_dataset_host = "../dataset/"

import nltk
from nltk import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.corpus import wordnet
import requests
from bs4 import BeautifulSoup
import emot
import pandas as pd

word_list = []
with open("../resource/words", 'r', encoding="utf8") as words_file:
    # with open("/code/resource/words", 'r', encoding="utf8") as words_file:
    for line in words_file.readlines():
        word_list.append(line.replace('\n', '').lower())

directory = directory_dataset_host
resp = requests.get("http://www.netlingo.com/acronyms.php")
soup = BeautifulSoup(resp.text, "html.parser")
slangdict = {}
key = ""
value = ""
for div in soup.findAll('div', attrs={'class': 'list_box3'}):
    for li in div.findAll('li'):
        for a in li.findAll('a'):
            key = a.text
            value = li.text.split(key)[1]
            slangdict[key] = value
slangdict.pop('banana', None)
slangdict.pop('word', None)
slangdict.pop('book', None)
slangdict.pop('hahaha', None)
directory_resource = directory_resource_host
with open(directory_resource + "slang_words.txt", 'r', encoding="utf8") as slang_file:
    slang = json.load(slang_file)
slangdict.update(slang)
for key in slangdict.keys():
    slangdict[key] = slangdict[key].replace('it means ', '').lower()
    slangdict[key] = slangdict[key].replace('f***', 'fuck')
    slangdict[key] = slangdict[key].replace('s***', 'shit')
    slangdict[key] = slangdict[key].replace('sh**', 'shit')
    slangdict[key] = slangdict[key].replace('f ***in', 'fuckin')
slangdict2 = {}
for key in slangdict.keys():
    if slangdict[key] != '':
        slangdict2[str(key).lower()] = slangdict[key].lower()
slangdict.clear()
slangdict.update(slangdict2)

list_marks = ["[", ",", "?", "!", ".", ";", ":", "\\", "/", "(", ")", "&", "_", "+", "=", "<", ">", "]", "-", "#"]

not_words = []
not_words_t = []
# directory_resource = directory_resource_docker
directory_resource = directory_resource_host
words_pos = []
for filename in os.listdir(directory_resource + "Pos/"):
    path_file = directory_resource + "Pos/" + filename
    with open(directory_resource + "Pos/" + filename, 'r', encoding="utf8") as input_file:
        for line in input_file.readlines():
            words_pos.append(line[:-2])
with open(directory_resource + "elenco-parole-che-negano-parole-successive.txt", 'r', encoding="utf8") as input_file:
    for line in input_file.readlines():
        if "\'t" in line:
            not_words_t.append(line[:-4])
        else:
            not_words.append(line[:-1])

lemmatizer = WordNetLemmatizer()


# elimina le parole USERNAME e URL dal dataset e crea dei file Processed_* che contengono i messaggi twitter puliti
def clean_dataset():
    # directory = directory_dataset_docker
    directory = "../dataset/"
    for filename in os.listdir(directory):
        if filename.startswith("dataset"):
            path_file = directory + filename
            with open(path_file, 'r', encoding="utf8") as input_file:
                with open(directory + "NO_USERURL_" + filename, 'w', encoding="utf8") as output_file:
                    for line in input_file.readlines():
                        line = line.replace("USERNAME", '')
                        new_line = line.replace("URL", '')
                        output_file.write(new_line)


def process_hashtag(hashtag_sequence, sentence):
    for index, element_word in enumerate(hashtag_sequence):
        list_words = []
        origin = element_word
        element_word = element_word[1:]
        while len(element_word) > 0:
            i = len(element_word) + 1
            while i > 1:
                i -= 1
                if element_word[:i] in word_list:
                    list_words.append(element_word[:i])
                    element_word = element_word[i:]
                    break
                elif len(re.findall("^[0-9]*$", element_word[:i])):
                    list_words.append(element_word[:i])
                    element_word = element_word[i:]
                    break
                elif element_word[:i] in list_marks:
                    element_word = element_word[i:]
        sub = ""
        for x in list_words:
            sub += str(x + ' ')
        sentence = sentence.replace(origin, sub)
    return sentence[:-1]


def process_slang(word):
    if word.lower() in slangdict.keys():
        word_tokenized_list = word_tokenize(slangdict[word])
    elif len(re.findall("^omg.+", word)) > 0:
        word_tokenized_list = word_tokenize(slangdict['omg'])
    elif len(re.findall("^lol.+", word)) > 0:
        # la parola inizia con lol
        word_tokenized_list = word_tokenize(slangdict['lol'])
    elif len(re.findall("^lmao.+", word)) > 0:
        word_tokenized_list = word_tokenize(slangdict['lmao'])
    elif len(re.findall("^haha.+", word)) > 0:
        # la parola iniza con haha
        word_tokenized_list = word_tokenize(slangdict['haha'])
    elif len(re.findall("^ahah.*", word)) > 0:
        word_tokenized_list = word_tokenize(slangdict['haha'])
    else:
        return word
    str_words = ""
    for word in word_tokenized_list:
        str_words += word + " "
    return str_words[:-1]


def process_mark(pos_sentence):
    pos_sentence_new = []
    for index, pos in enumerate(pos_sentence):
        flag = True
        # la parola è un segno di punteggiatura
        if pos[0] in list_marks or len(re.findall(".*\..*\..*", pos[0])) > 0:
            flag = False
        if pos[0] in not_words:
            flag = False
            flag2 = True
            i = 0
            index += 1
            while i < 2:
                if len(pos_sentence) > index + i and (
                        'V' in pos_sentence[index + i][1] or 'J' in pos_sentence[index + i][1]) \
                        and (lemmatizer.lemmatize(pos_sentence[index + i][0]) in words_pos):
                    flag2 = False
                    break
                i += 1
            if not flag2:
                index += 2
        elif pos[0] in not_words_t and len(pos_sentence) >= index + 2 and pos_sentence[index + 1:][0][0] == 't':
            flag = False
            flag2 = True
            index += 2
            i = 0
            while i < 2:
                if len(pos_sentence) > index + i and (
                        'V' in pos_sentence[index + i][1] or 'J' in pos_sentence[index + i][1]) \
                        and lemmatizer.lemmatize(pos_sentence[index + i][0]) in words_pos:
                    flag2 = False
                    break
                i += 1
            if not flag2:
                index += 2
        if flag:
            pos_sentence_new.append(pos)
    return pos_sentence_new


def process_stopword(sentence):
    new_sentence = []
    for pos in sentence:
        if pos[0] not in stopwords.words():
            new_sentence.append(pos)
    return new_sentence


def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return None


def process_sentence():
    with open(directory_resource_host + "emoticon_pos.txt", 'r', encoding="utf8") as input_file:
        x = input_file.readline()
    list_emoticon_resource = ast.literal_eval(x)

    with open(directory_resource_host + "emoticon_neg.txt", 'r', encoding="utf8") as input_file:
        y = input_file.readline()
    list_emoticon_resource.extend(ast.literal_eval(y))

    with open(directory_resource_host + "emoji.txt", 'r') as input_file:
        emoji = input_file.readline()
    emoji = emoji[1:-1].replace(',', ' ')
    list_emoji_resource = []
    for item in emoji.split():
        list_emoji_resource.append(item)

    with open(directory_resource_host + "emoji_neg.txt", 'r') as input_file:
        emoji = input_file.readline()
    emoji = emoji[1:-1].replace(',', ' ')
    for item in emoji.split():
        list_emoji_resource.append(item)

    with open(directory_resource_host + "emoji_pos.txt", 'r') as input_file:
        emoji = input_file.readline()
    emoji = emoji[1:-1].replace(',', ' ')
    for item in emoji.split():
        list_emoji_resource.append(item)

    dict_twitter = []

    directory = directory_dataset_host
    for filename in os.listdir(directory):
        if filename.startswith("NO_USERURL"):
            sentiment = filename[22:-8]
            path_file = directory + filename
            sent_list = []
            with open(path_file, 'r', encoding="utf8") as input_file:
                for line in input_file.readlines():
                    sent_list.append(line[:-2])
            list_hashtag = []
            list_emoji = []
            list_emoticon = []
            emot_obj = emot.emot()
            for index_sent, sentence in enumerate(sent_list):
                # process hashtag
                hashtag_sequence = re.findall("#[a-z0-9_]+", sentence)
                for hashtag in hashtag_sequence:
                    list_hashtag.append(hashtag)
                sentence = process_hashtag(hashtag_sequence, sentence)

                # process emoji
                found_emoji = emot_obj.emoji(string=sentence)
                for item in found_emoji['value']:
                    try:
                        while item in sentence:
                            list_emoji.append(item)
                            sentence = sentence.replace(item, '', 1)
                    except:
                        pass
                for item in list_emoji_resource:
                    item = item.encode('unicode_escape').decode('ascii')
                    while item in sentence:
                        list_emoji.append(item)
                        sentence = sentence.replace(item, '', 1)

                # process emoticon
                found_emoticon = emot_obj.emoticons(string=sentence)
                for item in found_emoticon['value']:
                    try:
                        while item in sentence:
                            list_emoticon.append(item)
                            sentence = sentence.replace(item, '', 1)
                    except:
                        pass
                for item in list_emoticon_resource:
                    while item in sentence:
                        list_emoticon.append(item)
                        sentence = sentence.replace(item, '', 1)

                # rimuovo i caratteri unicode
                sentence = re.sub(r'[^\x00-\x7f]', r'', sentence)

                # process_slang
                for word in word_tokenize(sentence):
                    no_slang = process_slang(word)
                    sentence.replace(word, no_slang, 1)

                # trasformo in lower case
                sentence = sentence.lower()

                # process pos_tagging
                sentence = nltk.pos_tag(word_tokenize(sentence))

                # process mark
                sentence = process_mark(sentence)

                # process lemming
                for index, pos in enumerate(sentence):
                    pos_tag = get_wordnet_pos(pos[1])
                    if pos_tag is not None:
                        sentence[index] = (lemmatizer.lemmatize(pos[0], pos_tag), pos[1])
                    else:
                        sentence[index] = (pos[0], pos[1])

                # process stop words
                sentence = process_stopword(sentence)

                # insert on a dictionary
                sent_element = {"sentiment": sentiment,
                                "doc_number": index_sent,
                                "words": [],
                                "hashtags": list_hashtag.copy(),
                                "emojis": list_emoji.copy(),
                                "emoticons": list_emoticon.copy()}

                word_already_inserted = []
                for pos in sentence:
                    occurences_word = 0
                    if pos[0] not in word_already_inserted:
                        for pos2 in sentence:
                            if pos[0] == pos2[0]:
                                occurences_word += 1
                        word_element = {"lemma": pos[0], "POS": pos[1],
                                        "freq": occurences_word,
                                        "in_lex_resources": {}}
                        sent_element["words"].append(word_element)
                        word_already_inserted.append(pos[0])
                dict_twitter.append(sent_element)
                list_hashtag.clear()
                list_emoji.clear()
                list_emoticon.clear()
    return dict_twitter


def scan_resources():
    resource = ""
    dictionary_result = {}
    # le risorse si trovano nella directory resource
    directory = directory_resource_host
    # directory = directory_resource_docker
    # per ogni directory dentro la directory resource
    for dir_name in os.listdir(directory):
        f = os.path.join(directory, dir_name)
        if os.path.isdir(f):
            # per ogni file txt, tsv, csv
            for filename in os.listdir(f + "/"):
                path_file = f + "/" + filename
                if "txt" in filename:
                    resource = filename[:-4].replace('-', '_')
                    # apro il file txt
                    with open(path_file, 'r') as file:
                        # per ogni linea del file
                        for line in file.readlines():
                            # se non è presente la parola all'interno del dizionario
                            if line.strip() not in dictionary_result.keys():
                                # inserisci la chiave nel dizionario e viene inizializzato la lista con il nome della
                                # risorsa come valore
                                dictionary_result[line.strip()] = {resource: 1}
                                dictionary_result[line.strip()]['word'] = word_tokenize(line)[0]
                            else:
                                # se la risorsa non è associata alla parola
                                if resource not in dictionary_result[line.strip()]:
                                    # inserisci la risorsa all'interno della lista dei valori associati alla parola
                                    # dictionary_result[line.strip()].append(filename[:-4])
                                    dictionary_result[line.strip()][resource] = 1
                                    dictionary_result[line.strip()]['word'] = word_tokenize(line)[0]
                elif "tsv" in filename:
                    with open(path_file, 'r') as file:
                        df = pd.read_csv(file, sep="\t", header=None)
                        for row in df.iterrows():
                            if row[1][0] not in dictionary_result.keys():
                                dictionary_result[row[1][0]] = {resource: 1}
                                dictionary_result[row[1][0]]['word'] = row[1][0]
                            else:
                                if resource not in dictionary_result[row[1][0]]:
                                    # dictionary_result[row[1][0]].append(filename[:-4])
                                    dictionary_result[row[1][0]][resource] = 1
                                    dictionary_result[row[1][0]]['word'] = row[1][0]
                else:
                    with open(path_file, 'r') as file:
                        df = pd.read_csv(file, sep=",")
                        for row in df.iterrows():
                            if row[0] not in dictionary_result.keys():
                                dictionary_result[row[0]] = {resource: 1}
                                dictionary_result[row[0]]['word'] = row[0]
                            else:
                                if resource not in dictionary_result[row[0]]:
                                    dictionary_result[row[0]][resource] = 1
                                    dictionary_result[row[0]]['word'] = row[0]
    compound_words = []
    for key in dictionary_result.keys():
        if '_' in str(key):
            compound_words.append(key)
    for word in compound_words:
        del dictionary_result[word]
    return dictionary_result


def lex_resources(dict_resources):
    list_lex = []
    list_resource = []
    dict_lex = {}
    for key in dict_resources.keys():
        for resource in dict_resources[key]:
            if resource not in list_resource and resource != "word":
                dict_lex["_id"] = resource
                dict_lex['sentiment'] = ""
                dict_lex['totNumberWords'] = 0
                list_resource.append(resource)
                list_lex.append(dict_lex.copy())
                dict_lex.clear()
    sentiment_list = ["neg", "pos", "like", "love", "anger", "anticipation", "afinn", "anew", "dal", "disgust", "hate",
                      "fear", "hope", "joy", "sadness", "trust", "surprise", ]
    # inserisco il nome del sentimento per ogni risorsa
    for resource in list_lex:
        for sentiment in sentiment_list:
            if sentiment in resource['_id'].lower():
                resource['sentiment'] = sentiment
    # inserisco il numero di parole per ogni risorsa
    for resource in list_lex:
        count_words = 0
        for key in dict_resources.keys():
            for resource2 in dict_resources[key]:
                if resource['_id'] == resource2:
                    count_words += 1
        resource['totNumberWords'] = count_words
    return list_lex


def lex_resources_words(dict_resources):
    dict_lex = {}
    list_lex = []
    for index, key in enumerate(dict_resources.keys()):
        dict_lex['_id'] = index
        dict_lex['lemma'] = key
        dict_lex['resources'] = []
        for resource in dict_resources[key]:
            if resource != "word":
                dict_lex["resources"].append({"$ref": "LexResources", "$id": resource})
        list_lex.append(dict_lex.copy())
        dict_lex.clear()
    return list_lex


def complete_dict(dict_twitter: list, dict_lex_resources):
    for index, doc_number in enumerate(dict_twitter):
        for sent in doc_number["words"]:
            for element in dict_lex_resources:
                if element["lemma"] == sent["lemma"]:
                    sent["in_lex_resources"] = {"$ref": "LexResurcesWords", "$id": element['_id']}
        with open("./dict_twitter_complete" + doc_number["sentiment"] + ".txt", 'w', encoding="utf8") as output_file:
            json.dump(dict_twitter[index], output_file)
    return dict_twitter


def main():
    #dict_resources = scan_resources()
    #dict_lex = lex_resources(dict_resources)
    #with open("./dict_lex_resources.txt", 'w', encoding="utf8") as output_file:
    #    json.dump(dict_lex, output_file)
    #dict_lex_words = lex_resources_words(dict_resources)
    #with open("./dict_lex_resources_words.txt", 'w', encoding="utf8") as output_file:
    #    json.dump(dict_lex_words, output_file)
    #dict_twitter = process_sentence()
    #with open("./dict_twitter.txt", 'w', encoding="utf8") as output_file:
    #    json.dump(dict_twitter, output_file)
    #dict_twitter.clear()
    with open("./dict_twitter.txt", 'r', encoding="utf8") as input_file:
        dict_twitter = json.load(input_file)
    with open("dict_lex_resources_words.txt", 'r', encoding="utf8") as input_file:
        dict_lex_words = json.load(input_file)
    dict_twitter = complete_dict(dict_twitter, dict_lex_words)
    with open("dict_twitter_complete.txt", 'w', encoding="utf8") as input_file:
        json.dump(dict_twitter, input_file)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
