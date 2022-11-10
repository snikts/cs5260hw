FROM python:3

WORKDIR /repository/


COPY . .

CMD [ "python", "consumer.py", "sqs", "usu-cs5260-sarahjones-bucket3", "--queueName=cs5260-requests" ]
