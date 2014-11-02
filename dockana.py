# vim:set noexpandtab:

# import pycurl, json
# from datetime import datetime
import os
import json
import argparse
import datetime
from elasticsearch import Elasticsearch
from docker import Client as DockerClient

# TODO: add a SIGCHLD handler to close connections when parent wants to kill children

class DMon:
	def __init__(self, elastic, docker):
		self.elastic = elastic
		self.docker  = docker

	def drink_events(self):
		for container in self.docker.containers():
			for event_json in self.docker.events():
				event = json.loads(event_json)
				# TODO: make this smart! -- enrichment
				if event['status'] == 'create':
					event["container"] = self.docker.inspect_container(event['id'])
				es_id = '{}-{}-{}'.format(event['status'], event['time'], event['id'])
				es_timestamp = datetime.datetime.fromtimestamp(event['time'])
				event['@timestamp'] = es_timestamp.isoformat()
				res = self.elastic.index(index='dmon', doc_type='event', body=json.dumps(event), id=es_id, timestamp=es_timestamp)

def create_dmon(elasticsearch_url, docker_url):
	es_host, es_port = elasticsearch_url.split(':')
	elastic = Elasticsearch(hosts=[{'host': es_host, 'port': es_port}])
	docker = DockerClient(base_url=docker_url)
	DMon(elastic, docker).drink_events()

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('elasticsearch', help='ES URL')
	parser.add_argument('docker', nargs='+', help='Docker URLs')
	args = parser.parse_args()

	children_dmons = []
	for docker_url in args.docker:
		pid = os.fork()
		if pid:
			children_dmons.append(pid)
		else:
			create_dmon(args.elasticsearch, docker_url)
			os._exit(0)
	for _, child in enumerate(children_dmons):
		os.waitpid(child, 0)

# - get json data from socket
#   - events socket is non-closing
#   - additional data is request-reply
# - parse json (func) and return an object to query
# - "enrich" an event message with json about the container/image related to that event
#    - query the /containers or /images api with the ID from the event
# - connect to elasticsearch
# - format an update to an elasticsearch index
# - use (same api?) socket to push json events to ES


# Usage:
#   dockana -e <ES_HOST+PORT> <DOCKER_SOCKET_URL>... 
#   dockana -e <ES_HOST+PORT> @<file_with_docker_SOCKET_URLs>
#   
#  dockana 127.0.0.1:9200 unix:/var/run/docker.sock
#

