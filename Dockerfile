FROM <nombre de la imagen de python>

RUN pip3 install aiomas==2.0.1

RUN pip3 install nest-asyncio==1.5.1

RUN pip3 install bs4

RUN pip3 install requests

COPY . /app

WORKDIR /app
