import ast
import os
import re
import emot
import json
import nltk
from bs4 import BeautifulSoup
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize
import pandas as pd
import psycopg2
import requests
from nltk.corpus import stopwords
from nltk.corpus import wordnet
from string import ascii_letters, punctuation

# nltk.download('wordnet')
# nltk.download('stopwords')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('omw-1.4')
# nltk.download('punkt')

directoy_docker = "/code/"
directory_dataset_docker = "/code/dataset/"
directory_resource_docker = "/code/resource/"
directory_resource_host = "../resource/"
directory_dataset_host = "../dataset/"


# scandisco le risorse lessicali
def scan_resources():
    resource = ""
    dictionary_result = {}
    # le risorse si trovano nella directory resource
    # directory = directory_resource_host
    directory = directory_resource_docker
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


# elimina gli hashtag dal testo
def process_hashtag():
    word_list = []
    dict_found_hashtag = {}
    list_marks = ["[", ",", "?", "!", ".", ";", ":", "\\", "/", "(", ")", "&", "_", "+", "=", "<", ">", "]", "#"]
    with open("../resource/words", 'r', encoding="utf8") as words_file:
        # with open("/code/resource/words", 'r', encoding="utf8") as words_file:
        for line in words_file.readlines():
            word_list.append(line.replace('\n', '').lower())
    # directory = directory_dataset_docker
    directory = "../dataset/"
    for filename in os.listdir(directory):
        if filename.startswith("NO_USERURL"):
            path_file = directory + filename
            with open(path_file, 'r', encoding="utf8") as input_file:
                with open(directory + "NO_HASHTAG_" + filename, 'w', encoding="utf8") as output_file:
                    for line in input_file.readlines():
                        hashtag_Sequence = re.findall("#[a-z0-9_]+", line)
                        for element_word in hashtag_Sequence:
                            if element_word in dict_found_hashtag.keys():
                                dict_found_hashtag[element_word] += 1
                            else:
                                dict_found_hashtag[element_word] = 1
                        for index, element_word in enumerate(hashtag_Sequence):
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
                            line = line.replace(origin, sub)
                        output_file.write(line)
    return dict_found_hashtag


# converte le emoji in testo
def process_emoji():
    with open(directory_resource_host + "emoji.txt", 'r') as input_file:
        emoji = input_file.readline()
    emoji = emoji[1:-1].replace(',', ' ')
    list_emoji = []
    for item in emoji.split():
        list_emoji.append(item)

    list_emoji_neg = []
    with open(directory_resource_host + "emoji_neg.txt", 'r') as input_file:
        emoji = input_file.readline()
    emoji = emoji[1:-1].replace(',', ' ')
    for item in emoji.split():
        list_emoji_neg.append(item)

    list_emoji_pos = []
    with open(directory_resource_host + "emoji_pos.txt", 'r') as input_file:
        emoji = input_file.readline()
    emoji = emoji[1:-1].replace(',', ' ')
    for item in emoji.split():
        list_emoji_pos.append(item)

    dict_found_emoji = {}
    dict_occurrence_emoji = {}
    directory = "../dataset/"
    # directory = directory_dataset_docker
    emot_obj = emot.emot()
    for filename in os.listdir(directory):
        if filename.startswith("NO_HASHTAG"):
            path_file = directory + filename
            with open(path_file, 'r', encoding="utf8") as input_file:
                with open(directory + "NO_EMOJI_" + filename, 'w', encoding="utf8") as output_file:
                    for line in input_file:
                        dictionary = emot_obj.emoji(string=line)
                        for index, item in enumerate(dictionary['value']):
                            try:
                                replace = str(dictionary['mean'][index]).replace(':', '')
                                replace = str(replace.replace('_', ' '))
                                if len(re.findall(item, line)) > 0:
                                    dict_found_emoji[item] = replace
                                    if item not in dict_occurrence_emoji.keys():
                                        dict_occurrence_emoji[item] = 1
                                    else:
                                        dict_occurrence_emoji[item] += 1
                                line = line.replace(item, '')
                                line = line.replace("\n", '')
                                line = line + replace + " " + "\n"
                            except:
                                pass

                        for emoji in list_emoji:
                            emoji = emoji.encode('unicode_escape').decode('ascii')
                            while emoji in line:
                                dict_found_emoji[emoji] = "others_emoji"
                                if emoji not in dict_occurrence_emoji.keys():
                                    dict_occurrence_emoji[emoji] = 1
                                else:
                                    dict_occurrence_emoji[emoji] += 1
                                line = line.replace(emoji, '', 1)

                        for emoji in list_emoji_pos:
                            while emoji in line:
                                dict_found_emoji[emoji] = "positive_emoji"
                                if emoji not in dict_occurrence_emoji.keys():
                                    dict_occurrence_emoji[emoji] = 1
                                else:
                                    dict_occurrence_emoji[emoji] += 1
                                line = line.replace(emoji, '', 1)

                        for emoji in list_emoji_neg:
                            dict_found_emoji[emoji] = "negative_emoji"
                            if emoji not in dict_occurrence_emoji.keys():
                                dict_occurrence_emoji[emoji] = 1
                            else:
                                dict_occurrence_emoji[emoji] += 1
                            line = line.replace(emoji, '', 1)
                        output_file.write(line)
    return dict_found_emoji, dict_occurrence_emoji


# converte le emoticons in testo
def process_emoticon():
    with open(directory_resource_host + "emoticon_pos.txt", 'r', encoding="utf8") as input_file:
        x = input_file.readline()
    list_emoticon_pos = ast.literal_eval(x)

    with open(directory_resource_host + "emoticon_neg.txt", 'r', encoding="utf8") as input_file:
        y = input_file.readline()
    list_emoticon_neg = ast.literal_eval(y)

    dict_found_emoticon = {}
    dict_occurrence_emoticon = {}
    # directory = directory_dataset_docker
    directory = "../dataset/"
    emot_obj = emot.emot()
    for filename in os.listdir(directory):
        if filename.startswith("NO_EMOJI"):
            path_file = directory + filename
            with open(path_file, 'r', encoding="utf8") as input_file:
                with open(directory + "NO_EMOTICON_" + filename, 'w', encoding="utf8") as output_file:
                    for line in input_file:
                        dictionary = emot_obj.emoticons(string=line)
                        for index, item in enumerate(dictionary['value']):
                            try:
                                replace = str(dictionary['mean'][index])
                                if len(re.findall(item, line)) > 0:
                                    dict_found_emoticon[item] = replace
                                    if item not in dict_occurrence_emoticon.keys():
                                        dict_occurrence_emoticon[item] = 1
                                    else:
                                        dict_occurrence_emoticon[item] += 1
                                line = line.replace(item, '')
                                line = line.replace('\n', '')
                                line = line + replace + "\n"
                            except:
                                pass

                        for emoticon in list_emoticon_pos:
                            while emoticon in line:
                                dict_found_emoticon[emoticon] = "positive_emoticon"
                                if emoticon not in dict_occurrence_emoticon.keys():
                                    dict_occurrence_emoticon[emoticon] = 1
                                else:
                                    dict_occurrence_emoticon[emoticon] += 1
                                line = line.replace(emoticon, '', 1)

                        for emoticon in list_emoticon_neg:
                            while emoticon in line:
                                dict_found_emoticon[emoticon] = "negative_emoticon"
                                if emoticon not in dict_occurrence_emoticon.keys():
                                    dict_occurrence_emoticon[emoticon] = 1
                                else:
                                    dict_occurrence_emoticon[emoticon] += 1
                                line = line.replace(emoticon, '', 1)
                        output_file.write(line)
    return dict_found_emoticon, dict_occurrence_emoticon


def process_lowercase():
    # directory = directory_dataset_docker
    directory = "../dataset/"
    for filename in os.listdir(directory):
        if filename.startswith("NO_EMOTICON"):
            path_file = directory + filename
            with open(path_file, 'r', encoding="utf8") as input_file:
                with open(directory + "LOWERCASE_" + filename, 'w', encoding="utf8") as output_file:
                    for line in input_file:
                        new_line = line.lower()
                        output_file.write(new_line)


def process_tokenizer():
    directory = directory_dataset_host
    # directory = directory_dataset_docker
    for filename in os.listdir(directory):
        if filename.startswith("LOWERCASE"):
            path_file = directory + filename
            with open(path_file, 'r', encoding="utf8") as input_file:
                list_output = []
                for line in input_file:
                    sentence = nltk.sent_tokenize(line, language="english")
                    for sent in sentence:
                        list_output.append(sent)
                with open(directory + "TOKENIZER_" + filename, 'w', encoding="utf8") as output_file:
                    json.dump(list_output, output_file)


def process_slang():
    directory = directory_dataset_host
    # directory = directory_dataset_docker
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
    # directory_resource = directory_resource_docker
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
    swapped_words = []
    for filename in os.listdir(directory):
        if filename.startswith("TOKENIZER"):
            path_file = directory + filename
            with open(path_file, 'r') as input_file:
                token_list = json.load(input_file)
                new_token_list = []
                for message in list(token_list):
                    message_tokenized = word_tokenize(message)
                    for index, word in enumerate(message_tokenized):
                        if word in slangdict.keys():
                            # la parola è presente nel dizionario degli slang
                            if word not in swapped_words:
                                swapped_words.append(word)
                            message_tokenized[index] = word_tokenize(slangdict[word])
                        if len(re.findall("^omg.+", word)) > 0:
                            # la parola inizia con omgg/omgg*
                            if word not in swapped_words:
                                swapped_words.append(word)
                            message_tokenized[index] = word_tokenize(slangdict['omg'])

                        if len(re.findall("^lol.+", word)) > 0:
                            # la parola inizia con lol
                            if word not in swapped_words:
                                swapped_words.append(word)
                            message_tokenized[index] = word_tokenize(slangdict['lol'])

                        if len(re.findall("^lmao.+", word)) > 0:
                            # la parola inizia con lmao
                            if word not in swapped_words:
                                swapped_words.append(word)
                            message_tokenized[index] = word_tokenize(slangdict['lmao'])

                        if len(re.findall("^haha.+", word)) > 0:
                            # la parola inizia con haha
                            if word not in swapped_words:
                                swapped_words.append(word)
                            message_tokenized[index] = word_tokenize(slangdict['haha'])

                        if len(re.findall("^ahah.*", word)) > 0:
                            if word not in swapped_words:
                                swapped_words.append(word)
                            message_tokenized[index] = word_tokenize(slangdict['haha'])

                    message_tokenized = str(message_tokenized).replace('[', '')
                    message_tokenized = message_tokenized.replace(']', '')
                    message_tokenized = "[" + message_tokenized + "]"
                    # trasformo la lista sottoforma di stringa in lista
                    new_token_list.append(ast.literal_eval(message_tokenized))
                sentence_list = []
                line = ""
                for message in new_token_list:
                    for word in message:
                        line += word + " "
                    sentence_list.append(line[:-1])
                    line = ""
                with open(directory + "NO_SLANG_" + filename, 'w', encoding="utf8") as output_file:
                    json.dump(sentence_list, output_file)
    with open("./swapped_slang_words.txt", 'w', encoding="utf8") as output_slang_file:
        for word in swapped_words:
            if len(re.findall("omg.+", word)) > 0:
                output_slang_file.write(word + " -> " + slangdict["omg"] + "\n")
            elif len(re.findall("lol.+", word)) > 0:
                output_slang_file.write(word + " -> " + slangdict['lol'] + "\n")
            elif len(re.findall("lmao.+", word)) > 0:
                output_slang_file.write(word + " -> " + slangdict['lmao'] + "\n")
            elif len(re.findall("haha.+", word)) > 0:
                output_slang_file.write(word + " -> " + slangdict['haha'] + "\n")
            elif len(re.findall("ahah.*", word)) > 0:
                output_slang_file.write(word + " -> " + slangdict['haha'] + "\n")
            else:
                output_slang_file.write(word + " -> " + slangdict[word] + "\n")


def process_english_words():
    directory = directory_dataset_host
    for filename in os.listdir(directory):
        if filename.startswith("NO_SLANG"):
            path_file = directory + filename
            with open(path_file, 'r', encoding="utf8") as input_file:
                token_list = list(json.load(input_file))
                english_words = []
                for sent in token_list:
                    english_words.append(re.sub(r'[^\x00-\x7f]', r'', sent))
                with open(directory + "ENGLISH_" + filename, 'w', encoding="utf8") as output_file:
                    json.dump(english_words, output_file)


def process_pos_tagging():
    directory = directory_dataset_host
    # directory = directory_dataset_docker
    for filename in os.listdir(directory):
        if filename.startswith("ENGLISH"):
            path_file = directory + filename
            with open(path_file, 'r', encoding="utf8") as input_file:
                token_list = list(json.load(input_file))
                pos_list = []
                for sent in token_list:
                    x = nltk.pos_tag(nltk.word_tokenize(sent))
                    if len(x) > 0:
                        pos_list.append(nltk.pos_tag(nltk.word_tokenize(sent)))
                with open(directory + "POS_" + filename, 'w', encoding="utf8") as output_file:
                    json.dump(pos_list, output_file)


def process_mark():
    lemmatizer = WordNetLemmatizer()
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
    with open(directory_resource + "elenco-parole-che-negano-parole-successive.txt", 'r',
              encoding="utf8") as input_file:
        for line in input_file.readlines():
            if "\'t" in line:
                not_words_t.append(line[:-4])
            else:
                not_words.append(line[:-1])
    directory = directory_dataset_host
    # directory = directory_dataset_docker
    list_marks = ["[", ",", "?", "!", ".", ";", ":", "\\", "/", "(", ")", "&", "_", "+", "=", "<", ">", "]", "-", "#"]
    for filename in os.listdir(directory):
        if filename.startswith("POS_"):
            path_file = directory + filename
            token_list = [json.loads(line) for line in open(path_file, 'r')]
            new_token_list = []
            for token in token_list:
                new_token_list.extend(token)
            token_list = []
            indexList = 0
            token_list.append([])
            for message in new_token_list:
                if len(token_list[indexList]) > 0:
                    indexList += 1
                    token_list.append([])
                for index, pos in enumerate(message):
                    flag = True
                    # la parola è un segno di punteggiatura
                    if pos[0] in list_marks or len(re.findall(".*\..*\..*", pos[0])) > 0:
                        flag = False
                    # la parola nega un'altra parola => elimino le due parole successive
                    if pos[0] in not_words:
                        flag = False
                        flag2 = True
                        i = 0
                        index += 1
                        while i < 2:
                            if len(message) > index + i and (
                                    'V' in message[index + i][1] or 'J' in message[index + i][1]) \
                                    and (lemmatizer.lemmatize(message[index + i][0]) in words_pos):
                                flag2 = False
                                break
                            i += 1
                        if not flag2:
                            index += 2
                    elif pos[0] in not_words_t and len(message) >= index + 2 and message[index + 1:][0][0] == 't':
                        flag = False
                        flag2 = True
                        index += 2
                        i = 0
                        while i < 2:
                            if len(message) > index + i and (
                                    'V' in message[index + i][1] or 'J' in message[index + i][1]) \
                                    and (lemmatizer.lemmatize(message[index + i][0]) in words_pos):
                                flag2 = False
                                break
                            i += 1
                        if not flag2:
                            index += 2
                    if flag:
                        token_list[indexList].append(pos)

            with open(directory + "NO_MARKS_" + filename, 'w', encoding="utf8") as output_file:
                json.dump(token_list, output_file)


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


def process_lemming():
    lemmatizer = WordNetLemmatizer()
    directory = directory_dataset_host
    # directory = directory_dataset_docker
    for filename in os.listdir(directory):
        if filename.startswith("NO_MARKS_"):
            path_file = directory + filename
            token_list = [json.loads(line) for line in open(path_file, 'r')]
            token_list = token_list[0]
            for message in token_list:
                for index, word in enumerate(message):
                    pos_tag = get_wordnet_pos(word[1])
                    if pos_tag is not None:
                        message[index] = (lemmatizer.lemmatize(word[0], pos_tag), word[1])
                    else:
                        message[index] = (word[0], word[1])

            with open(directory + "LEMM_" + filename, 'w', encoding="utf8") as output_file:
                json.dump(token_list, output_file)


def process_stop_words():
    # directory = directory_dataset_docker
    directory = directory_dataset_host
    for filename in os.listdir(directory):
        if filename.startswith("LEMM"):
            path_file = directory + filename
            token_list = [json.loads(line) for line in open(path_file, 'r')]
            token_list = token_list[0]
            new_token_list = []
            indexList = 0
            new_token_list.append([])
            dim = len(token_list)
            i = len(token_list)
            for index, message in enumerate(token_list):
                if len(new_token_list[indexList]) > 0:
                    indexList += 1
                    new_token_list.append([])
                for word in message:
                    if word[0] not in stopwords.words():
                        new_token_list[indexList].append(word)
                x = dim - index
                if i == x:
                    print(x)
                    i -= 10000
            with open(directory + "NO_STOPWORD_" + filename, 'w', encoding="utf8") as output_file:
                json.dump(new_token_list, output_file)


def store_to_db_rel():
    header = {"NRC_trust": 0, "EmoSN_anger": 0, "sentisense_disgust": 0, "HL_negatives": 0,
              "LIWC_NEG": 0, "NRC_anger": 0, "NRC_fear": 0, "NRC_surprise": 0, "afinn": 0, "NRC_sadness": 0,
              "listPosEffTerms": 0,
              "sentisense_anger": 0, "GI_NEG": 0, "NRC_anticipation": 0, "NRC_disgust": 0, "listNegEffTerms": 0,
              "sentisense_sadness": 0, "sentisense_fear": 0, "GI_POS": 0, "HL_positives": 0, "LIWC_POS": 0,
              "NRC_joy": 0, "sentisense_joy": 0,
              "sentisense_anticipation": 0, "EmoSN_joy": 0, "sentisense_like": 0, "sentisense_hate": 0,
              "sentisense_love": 0, "sentisense_hope": 0,
              "sentisense_surprise": 0, "frequency": 1, "emoji": False, "emoticon": False, "resource_bool": False,
              "pos": '', "word": ''}
    dict_to_db = {}
    directory = directory_dataset_docker
    for filename in os.listdir(directory):
        if filename.startswith("NO_STOPWORD"):
            path_file = directory + filename
            with open(path_file, 'r', encoding="utf8") as input_file:
                token_list = json.load(input_file)
                for message in token_list:
                    for word in message:
                        token = word[0] + "-" + word[1]
                        if token in dict_to_db.keys():
                            dict_to_db[token]['frequency'] += 1
                        else:
                            dict_to_db[token] = header.copy()
                            dict_to_db[token]['word'] = word[0]
                            dict_to_db[token]['pos'] = word[1]
    with open("/code/dict_found_hashtag.txt", 'r', encoding="utf8") as input_file:
        dict_hashtag = json.load(input_file)

    for key in dict_hashtag:
        dict_to_db[key] = header.copy()
        dict_to_db[key]['word'] = key
        dict_to_db[key]['frequency'] = dict_hashtag[key]

    with open("/code/dict_occurrences_emoji.txt", 'r', encoding="utf8") as input_file:
        dict_emoji = json.load(input_file)

    for key in dict_emoji:
        dict_to_db[key] = header.copy()
        dict_to_db[key]['word'] = key
        dict_to_db[key]['frequency'] = dict_emoji[key]
        dict_to_db[key]['emoji'] = True

    with open("/code/dict_occurrences_emoticon.txt", 'r', encoding="utf8") as input_file:
        dict_emoticon = json.load(input_file)

    for key in dict_emoticon:
        dict_to_db[key] = header.copy()
        dict_to_db[key]['word'] = key
        dict_to_db[key]['frequency'] = dict_emoticon[key]
        dict_to_db[key]['emoticon'] = True

    dictionary_resource = scan_resources()
    for key in dictionary_resource.keys():
        if key in dict_to_db.keys():
            dict_to_db[key]['resource_bool'] = True
        else:
            dict_to_db[key] = header.copy()
            dict_to_db[key]['frequency'] = 0
            dict_to_db[key]['word'] = key
        for resource_name in dictionary_resource[key].keys():
            if resource_name != 'word':
                dict_to_db[key][resource_name] = 1
    sql = ""
    for resource in header:
        if resource not in ['emoji', 'emoticon', 'word', 'pos', 'resource_bool']:
            sql += resource + " INTEGER, "
    sql += "emoji BOOLEAN, emoticon BOOLEAN, resource_bool BOOLEAN, pos VARCHAR(10), word VARCHAR(500), "
    conn = psycopg2.connect(
        database="relational-db", user='postgres', password='postgres', host='db', port='5432'
    )
    cursor = conn.cursor()
    cursor.execute("select version()")
    data = cursor.fetchone()
    print("Connection established to: ", data)
    cursor.execute("CREATE TABLE word_frequency(" + sql + "PRIMARY KEY (word, pos));")

    insert_statement = '''INSERT INTO word_frequency VALUES(%(NRC_trust)s, %(EmoSN_anger)s, %(sentisense_disgust)s, %(HL_negatives)s, %(LIWC_NEG)s, %(NRC_anger)s, %(NRC_fear)s, %(NRC_surprise)s, %(afinn)s, %(NRC_sadness)s, %(listPosEffTerms)s, %(sentisense_anger)s, %(GI_NEG)s, %(NRC_anticipation)s, %(NRC_disgust)s, %(listNegEffTerms)s, %(sentisense_sadness)s, %(sentisense_fear)s, %(GI_POS)s, %(HL_positives)s, %(LIWC_POS)s, %(NRC_joy)s, %(sentisense_joy)s, %(sentisense_anticipation)s, %(EmoSN_joy)s, %(sentisense_like)s, %(sentisense_hate)s, %(sentisense_love)s, %(sentisense_hope)s, %(sentisense_surprise)s, %(frequency)s, %(emoji)s, %(emoticon)s, %(resource_bool)s, %(pos)s, %(word)s)'''

    for key in dict_to_db.keys():
        cursor.execute(insert_statement, dict_to_db[key])

    conn.commit()
    conn.close()


def main():
    clean_dataset()
    dict_found_hashtag = process_hashtag()
    with open("./dict_found_hashtag.txt", 'w', encoding="utf8") as output_file:
        json.dump(dict_found_hashtag, output_file)
    dict_found_emoji, dict_occurrences_emoji = process_emoji()
    with open("./dict_found_emoji.txt", 'w', encoding="utf8") as output_file:
        json.dump(dict_found_emoji, output_file)
    with open("./dict_occurrences_emoji.txt", 'w', encoding="utf8") as output_file:
        json.dump(dict_occurrences_emoji, output_file)
    dict_found_emoticon, dict_occurrences_emoticon = process_emoticon()
    with open("./dict_found_emoticon.txt", 'w', encoding="utf8") as output_file:
        json.dump(dict_found_emoticon, output_file)
    with open("./dict_occurrences_emoticon.txt", 'w', encoding="utf8") as output_file:
        json.dump(dict_occurrences_emoticon, output_file)
    process_lowercase()
    process_tokenizer()
    process_slang()
    process_english_words()
    process_pos_tagging()
    process_mark()
    process_lemming()
    process_stop_words()
    # store_to_db_rel()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
