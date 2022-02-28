FROM python:3.8
RUN apt-get update && mkdir importservice/

# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /importservice/requirements.txt
RUN pip3 install -r /importservice/requirements.txt

COPY ./main.py /importservice/main.py
WORKDIR /importservice

ENV PYTHONUNBUFFERED=1

CMD ["python3","main.py"]
