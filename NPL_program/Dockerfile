FROM python:latest
WORKDIR /code
COPY app/. .
COPY resource resource
COPY dataset dataset
VOLUME /code
RUN pip install -r requirements.txt
CMD ["python", "-u", "./main.py"]