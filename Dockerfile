FROM python:3

EXPOSE 8000

WORKDIR /usr/src/app
VOLUME /usr/src/app/config

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

HEALTHCHECK --interval=2m --timeout=3s CMD curl -f http://localhost:8000/ || exit 1

COPY src .

CMD [ "python", "./webhook2influxdb.py" ]
