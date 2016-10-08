#!/usr/bin/env python3
import json
import settings
import etcd
import docker
import logging
import sys
import time

logging.basicConfig(filename=settings.watcher_log, level=4, format=u'%(asctime)s  %(message)s', )
ch = logging.StreamHandler(sys.stdout)


class Watcher:
    def __init__(self, etcd_host, etcd_port, etcd_prefix, etcd_hosts_prefix, etcd_pods_prefix):
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self.etcd_prefix = etcd_prefix
        self.etcd_hosts_prefix = etcd_hosts_prefix
        self.etcd_pods_prefix = etcd_pods_prefix


    def get_pods_list(self):
        etcd_client = etcd.Client(host=self.etcd_host, port=self.etcd_port)
        pods_list = {}
        pods_total = 0
        pods_ls = etcd_client.get(settings.etcd_prefix + settings.etcd_pods_prefix)
        for pod_temp in pods_ls.children:
            pods_total += 1

        if pods_total:
            for pod in pods_ls.children:
                pods_list[pod.key] = pod.value
        return pods_list

    def get_hosts_list(self):
        etcd_client = etcd.Client(host=self.etcd_host, port=self.etcd_port)
        hosts_list = {}
        hosts_total = 0
        hosts_ls = etcd_client.get(settings.etcd_prefix + settings.etcd_hosts_prefix)
        for host_temp in hosts_ls.children:
            hosts_total += 1
        if hosts_total:
            for host in hosts_ls.children:
                hosts_list[host.key] = host.value
        return hosts_list

    def get_host_name(self, input_str):
        return input_str.split('/')[-1]

    def run_container(self, host, port, image):
        pass

    def get_all_running_containers(self):
        self.all_running_containers = {}
        for host_path, host_json_str in self.hosts_list.items():
            if self.get_host_name(host_path) not in self.all_running_containers:
                self.all_running_containers[self.get_host_name(host_path)] = []
            host_dict = json.loads(host_json_str)
            cli = docker.Client(base_url='tcp://' + host_dict['host'] + ':' + host_dict['port'], version=settings.docker_version)
            docker_containers_list = cli.containers()
            for container in docker_containers_list:
                self.all_running_containers[self.get_host_name(host_path)] += container['Id']
        logging.debug(self.all_running_containers)

    def update_hosts_with_running_containers(self):
        etcd_client = etcd.Client(host=self.etcd_host, port=self.etcd_port)
        for host_path, host_json_str in self.hosts_list.items():
            host_json_str_etcd = etcd_client.get(
                settings.etcd_prefix + settings.etcd_hosts_prefix + self.get_host_name(host_path))
            json_host = json.loads(host_json_str_etcd.value)
            json_host['containers'] = self.all_running_containers[self.get_host_name(host_path)]
            etcd_client.set(host_path, json.dumps(json_host))

    def schedule(self):
        etcd_client = etcd.Client(host=self.etcd_host, port=self.etcd_port)


    def watch(self):
        while (True):
            self.pods_list = self.get_pods_list()
            self.hosts_list = self.get_hosts_list()
            self.get_all_running_containers()
            self.update_hosts_with_running_containers()
            self.schedule()
            time.sleep(settings.watcher_sleep)


if __name__ == '__main__':
    watcher = Watcher(settings.etcd_host, settings.etcd_port,
                      settings.etcd_prefix, settings.etcd_hosts_prefix,
                      settings.etcd_pods_prefix)
    watcher.watch()
