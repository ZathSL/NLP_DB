FROM python:latest
WORKDIR /code
COPY calc_stats.py .
COPY dict_twitter_complete.txt .
COPY dict_lex_resources.txt .
COPY dict_lex_resources_words.txt .
COPY requirements.txt .
COPY NotoEmoji.ttf .
COPY Twitter_bird_logo.png .
VOLUME /code
RUN pip install -r requirements.txt
CMD ["python", "-u", "./calc_stats.py"]