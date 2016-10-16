#!/usr/bin/env python3
import json
import settings
import etcd
import docker
import logging
import sys
import time
import ipdb

logging.basicConfig(filename=settings.watcher_log, level=settings.log_level, format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s %(asctime)s  %(message)s', )
ch = logging.StreamHandler(sys.stdout)


class Watcher:
    def __init__(self, etcd_host, etcd_port, etcd_prefix, etcd_hosts_prefix, etcd_pods_prefix):
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self.etcd_prefix = etcd_prefix
        self.etcd_hosts_prefix = etcd_hosts_prefix
        self.etcd_pods_prefix = etcd_pods_prefix
        self.pods_list = {}
        self.hosts_list = {}

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
        logging.warning(pods_list)
        self.pods_list = pods_list

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
        self.hosts_list = hosts_list

    def get_host_name(self, input_str):
        return input_str.split('/')[-1]

    def run_container(self, pod, host, port, image, memory):
        cli = docker.Client(base_url='tcp://' + host + ':' + port,
                            version=settings.docker_version)
        id_dict = cli.create_container(image=image)
        container_id = id_dict['Id']
        cli.start(container_id)
        for pod_path, pod_json in self.pods_list.items():
            if self.get_host_name(pod_path) == pod:
                pod_json_json = json.loads(pod_json)
                pod_json_json['containers_list'] .append(str(container_id))
                self.pods_list[pod_path] = json.dumps(pod_json_json)
        for host_path, host_json in self.hosts_list.items():
            if self.get_host_name(host_path) == host:
                host_json_json = json.loads(host_json)
                host_json_json['containers'].append(str(container_id))
                self.hosts_list[host_path] = json.dumps(host_json_json)
        return container_id

    def stop_container(self, host, port, container_id):
        cli = docker.Client(base_url='tcp://' + host + ':' + port,
                            version=settings.docker_version)
        cli.stop(container_id)
        cli.remove_container(container_id)
        return True

    def get_all_running_containers(self):
        self.all_running_containers = {}
        for host_path, host_json_str in self.hosts_list.items():
            if self.get_host_name(host_path) not in self.all_running_containers:
                self.all_running_containers[self.get_host_name(host_path)] = []
            host_dict = json.loads(host_json_str)
            cli = docker.Client(base_url='tcp://' + host_dict['host'] + ':' + host_dict['port'],
                                version=settings.docker_version)
            docker_containers_list = cli.containers()
            for container in docker_containers_list:
                self.all_running_containers[self.get_host_name(host_path)] += container['Id']
    def check_if_container_fits_on_host(self, host, pod):
        host = self.hosts_list[host]
        pod = self.pods_list[pod]
        host_json = json.loads(host)
        pod_json = json.loads(pod)
        host_cpus = int(host_json['cpus'])
        pod_cpus = int(pod_json['cpus'])
        host_memory = int(host_json['memory'])
        pod_memory = int(pod_json['memory'])
        host_disk = int(host_json['disk'])
        pod_disk = int(host_json['disk'])
        if host_cpus >= pod_cpus and host_memory >= pod_memory and host_disk >= pod_disk:
            return True
        return False

    def update_host_config_minus(self, host, pod, container_id):
        host = self.hosts_list[host]
        pod = self.pods_list[pod]
        host_json = json.loads(host)
        pod_json = json.loads(pod)
        host_cpus = int(host_json['cpus'])
        pod_cpus = int(pod_json['cpus'])
        host_memory = int(host_json['memory'])
        pod_memory = int(pod_json['memory'])
        host_disk = int(host_json['disk'])
        pod_disk = int(host_json['disk'])
        new_host_cpus = host_cpus - pod_cpus
        new_host_memory = host_memory - pod_memory
        new_host_disk = host_disk - pod_disk
        host_json['cpus'] = new_host_cpus
        host_json['memory'] = new_host_memory
        host_json['disk'] = new_host_disk
        host_json['containers'].append(str(container_id))
        self.hosts_list[host] = json.dumps(host_json)

    def update_host_config_plus(self, host, pod, container_id):
        host = self.hosts_list[host]
        pod = self.pods_list[pod]
        host_json = json.loads(host)
        pod_json = json.loads(pod)
        host_cpus = int(host_json['cpus'])
        pod_cpus = int(pod_json['cpus'])
        host_memory = int(host_json['memory'])
        pod_memory = int(pod_json['memory'])
        host_disk = int(host_json['disk'])
        pod_disk = int(host_json['disk'])
        new_host_cpus = host_cpus + pod_cpus
        new_host_memory = host_memory + pod_memory
        new_host_disk = host_disk + pod_disk
        host_json['cpus'] = new_host_cpus
        host_json['memory'] = new_host_memory
        host_json['disk'] = new_host_disk
        host_json['containers'] -= container_id
        self.hosts_list[host] = json.dumps(host_json)

    def find_container_host(self, container_id):
        for host_path, host_json_str in self.hosts_list.items():
            host_json_json = json.loads(host_json_str)
            if container_id in host_json_json['containers']:
                return host_json_json['host'], host_json_json['port']
        return False, False

    def schedule(self):
        for pod_path, pod_json in self.pods_list.items():
            pod_name = self.get_host_name(pod_path)
            pod_json_json = json.loads(pod_json)
            pod_containers = int(pod_json_json['containers'])
            running_containers = len(pod_json_json['containers_list'])
            if pod_containers > running_containers:
                run = False
                for host_path, host_json in self.hosts_list.items():
                    if self.check_if_container_fits_on_host(host_path, pod_path):
                        host_json_json = json.loads(host_json)
                        host = host_json_json['host']
                        port = host_json_json['port']
                        image = pod_json_json['image']
                        memory = pod_json_json['memory']
                        container_id = self.run_container(pod_name, host, port, image, memory)
                        run = True
                        self.update_host_config_minus(host_path, pod_path, container_id)
                        logging.warning('run ' + pod_name + ' container')
                        break
                    break
                if not run:
                    logging.error('unable to run pod ' + pod_name + ' on host ' + host)
            elif pod_containers < running_containers:
                container_id = pod_json_json['containers_list'][0]
                container_host, container_port = self.find_container_host(container_id)
                if container_host != False and container_port != False:
                    result = self.stop_container(container_host, container_port, container_id)
                    logging.warning('removed ' + container_id + ' from ' + container_host)
                break
            break

    def write_all_to_etcd(self):
        logging.warning(self.pods_list)
        logging.warning(self.hosts_list)
        etcd_client = etcd.Client(host=self.etcd_host, port=self.etcd_port)
        for pod_path, pod_json in self.pods_list.items():
            etcd_client.set(pod_path, json.dumps(pod_json))
        for host_path, host_json in self.hosts_list.items():
            logging.error(host_path)
            logging.error(host_json)
            etcd_client.set(host_path, json.dumps(host_json))

    def watch(self):
        while (True):
            logging.warning('starting watcher')
            self.get_pods_list()
            self.get_hosts_list()
            self.get_all_running_containers()
            self.schedule()
            self.write_all_to_etcd()
            logging.warning('sleep ' + str(settings.watcher_sleep) + ' seconds')
            time.sleep(settings.watcher_sleep)


if __name__ == '__main__':
    watcher = Watcher(settings.etcd_host, settings.etcd_port,
                      settings.etcd_prefix, settings.etcd_hosts_prefix,
                      settings.etcd_pods_prefix)
    watcher.watch()
