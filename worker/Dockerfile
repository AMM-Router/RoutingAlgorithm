FROM python
RUN pip install pika
COPY ./worker.py /worker.py

CMD  python worker.py