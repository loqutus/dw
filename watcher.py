#!/usr/bin/env python3
import json
import settings
import etcd
import docker
import logging
import sys
import time

logging.basicConfig(filename=settings.watcher_log, level=settings.log_level, format=u'%(asctime)s  %(message)s', )
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

    def run_container(self, pod, host, port, image, memory):
        cli = docker.Client(base_url='tcp://' + host + ':' + port,
                            version=settings.docker_version)
        id_dict = cli.create_container(image=image)
        container_id = id_dict['Id']
        cli.start(container_id)
        for pod_path, pod_json in self.pods_list.items():
            if self.get_host_name(pod_path) == pod:
                pod_json_json = json.loads(pod_json)
                pod_json_json['containers_list'] += container_id
                self.pods_list[pod_path] = json.dumps(pod_json_json)
        for host_path, host_json in self.host_list.items():
            if self.get_host_name(host_path) == host:
                host_json_json = json.loads(host_json)
                host_json_json['containers_list'] += container_id
                self.hosts_list[host_path] = json.dumps(host_json_json)
        return container_id

    def stop_container(self, host, container_id):
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
        if host_cpus >= pod_cpus and host_memory > pod_memory and host_disk > pod_disk:
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
        host_json['containers'] += container_id
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
                return host_path
        return False

    def schedule(self):
        for pod_path, pod_json in self.pods_list.items():
            pod_name = self.get_host_name(pod_path)
            pod_json_json = json.loads(pod_json)
            pod_containers = int(pod_json_json['containers'])
            host_containers = len(pod_json_json['containers_list'])
            if pod_containers > host_containers:
                runs = pod_containers - host_containers
                i = 0
                while i < runs:
                    run = False
                    for host_path, host_json in self.hosts_list.items():
                        if self.check_if_container_fits_on_host(host_path, pod_path):
                            host_json_json = json.dumps(host_json)
                            host = host_json_json['host']
                            port = host_json_json['port']
                            image = host_json_json['image']
                            memory = host_json_json['memory']
                            container_id = self.run_container(pod_name, host, port, image, memory)
                            run = True
                            self.update_host_config_minus(host_path, pod_path, container_id)
                            runs -= 1
                            logging.warning('run ' + self.get_host_name(host_path) + ' container')
                    if not run:
                        logging.error('unable to run pod ' + pod_name)
            elif pod_containers < host_containers:
                runs = host_containers - pod_containers
                run = False
                while i < runs:
                    container_id = pod_json_json['containers_list'][0]
                    container_host = self.find_container_host(container_id)
                    result = self.stop_container(container_host, container_id)
                    run = True
                if not run:
                    logging.error('unable to delete pod ' + pod_name)



    def write_all_to_etcd(self):
        logging.warning(self.pods_list)
        logging.warning(self.hosts_list)
        etcd_client = etcd.Client(host=self.etcd_host, port=self.etcd_port)
        for pod_path, pod_json in self.pods_list.items():
            etcd_client.set(pod_path, pod_json)
        for host_path, host_json in self.hosts_list.items():
            etcd_client.set(host_path, host_json)


    def watch(self):
        while (True):
            logging.warning('starting watcher')
            self.pods_list = self.get_pods_list()
            self.hosts_list = self.get_hosts_list()
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
