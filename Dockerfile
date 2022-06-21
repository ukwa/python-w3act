FROM python:3.7-slim

WORKDIR /usr/src/w3act

RUN pip install importlib_metadata

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN python setup.py install

CMD w3act


