#!/usr/bin/env python3

import logging
import yaml
import os
import json
import sys
import hashlib
import urllib.parse
from influxdb import InfluxDBClient
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


class Webhook2Influxdb:

    def __init__(self):
        config_file = os.path.join("config", "webhooks.yml")
        with open(config_file) as file:
            logging.debug("Loading locations from %s" % config_file)
            self.config = yaml.load(file, Loader=yaml.FullLoader)

        self.influx_clients = {}
        for client in self.config:
            self.setup_influx_client(client['host'], client['port'], client['user'], client['password'], client['db'],
                                     client['measurement'])

    @staticmethod
    def hash_influx_server(host, port, user, db, measurement):
        hash_source = "%s%s%s%s%s" % (host, port, user, db, measurement)
        h = hashlib.sha1(hash_source.encode('utf-8'))
        return h.hexdigest()

    def setup_influx_client(self, host, port, user, password, db, measurement):
        hash_name = self.hash_influx_server(host, port, user, db, measurement)
        logging.info('Connecting to influx on %s:%s as %s to db %s as %s' % (host, port, user, db, hash_name))
        influx_client = InfluxDBClient(host=host, port=port, username=user, password=password, database=db)
        influx_client.create_database(db)
        self.influx_clients[hash_name] = {}
        self.influx_clients[hash_name]['client'] = influx_client
        self.influx_clients[hash_name]['host'] = host
        self.influx_clients[hash_name]['port'] = port
        self.influx_clients[hash_name]['user'] = user
        self.influx_clients[hash_name]['db'] = db
        self.influx_clients[hash_name]['measurement'] = measurement

    def list_clients(self):
        ret = []
        for client in sorted(self.influx_clients.keys()):
            ret.append({'hash': str(client),
                        'host': str(self.influx_clients[client]['host']),
                        'port': str(self.influx_clients[client]['port']),
                        'user': str(self.influx_clients[client]['user']),
                        'db': str(self.influx_clients[client]['db']),
                        'measurement': str(self.influx_clients[client]['measurement']),
                        })
        return ret

    def work(self, hook, values):

        try:
            sensor_id = "sonoff-%s" % msg.topic.split("/")[-2]

            if sensor_id in self.sensor_names.keys():
                sensor_name = self.sensor_names[sensor_id]['sensor_name']
                temp_diff = float(self.sensor_names[sensor_id]['temp_diff'])
            else:
                logging.warning("Sensor <%s> not found in sensor_names.yml. Configured sensors: %s" % (sensor_id, ", ".join(self.sensor_names.keys())))
                sensor_name = sensor_id
                temp_diff = 0.0

            json_body = [
            {
                "measurement": hook['measurement'],
                "tags": {
                    "sensor_id": sensor_id,
                    "sensor_name": sensor_name,
                },
                "fields": {
                    "temperature": float(msg.payload.decode("utf-8")) + temp_diff,
                }
            }
            ]

            res = self.influx_client.write_points(json_body)
            self.write_counter += 1
            if self.write_counter % self.pagination == 0:
                logging.info('Wrote %s sets of data to influxdb (res: %s)' % (self.write_counter, res))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            emsg = ''.join('' + line for line in lines)
            logging.warning('Caught exception on JSON data reading: \n%s' % (emsg))


w2i = Webhook2Influxdb()


class StaticServer(BaseHTTPRequestHandler):

    def execute_request(self, data_string):
        data = urllib.parse.parse_qs(data_string)
        hook_found = False
        for hook in w2i.list_clients():
            if hook['hash'] in self.path:
                hook_found = True
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                w2i.work(hook, data)

        if not hook_found:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Error: Webhook not found")

    def do_POST(self):
        logging.info("Handling POST request from %s" % self.address_string())
        content_len = int(self.headers.get('Content-Length'))
        post_body = self.rfile.read(content_len)
        self.execute_request(post_body.decode('utf-8'))

    def do_GET(self):
        logging.info("Handling GET request from %s" % self.address_string())
        # list hook index
        if self.path == "/":
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            ret = [b"<html><head><title>Webhook list</title></head><body><table>",
                   b"<tr><th>URL</th><th>Influx server</th><th>Database</th><th>Measurement</th></tr>"]

            for hook in w2i.list_clients():
                ret.append(b"<tr><td>%s</td><td>%s@%s:%s</td><td>%s</td><td>%s</td></tr>" % (
                    hook['hash'].encode('utf-8'),
                    hook['user'].encode('utf-8'),
                    hook['host'].encode('utf-8'),
                    hook['port'].encode('utf-8'),
                    hook['db'].encode('utf-8'),
                    hook['measurement'].encode('utf-8')))

            ret.append(b"</table></body></html>")

            self.wfile.write(b"".join(ret))
        else:
            data_start = self.path.find("?")
            if data_start > 0:
                data_string = self.path[self.path.find("?") + 1:]
                self.execute_request(data_string)
            else:
                self.send_response(406)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Error: No data found")


if __name__ == '__main__':
    server_class = HTTPServer
    handler_class = StaticServer
    port = 8000
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.debug("Running in server mode on port %s" % port)
    httpd.serve_forever()
