#!/usr/bin/env python3
from flask import Flask, request, render_template
import json
import settings
import etcd
import logging
import sys

logging.basicConfig(filename=settings.web_log, level=30,
                    format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', )
ch = logging.StreamHandler(sys.stdout)
logging.warning('starting web')

app = Flask(__name__)


@app.route('/add_host', methods=['POST'])
def add_host_api():
    logging.debug('add_host')
    host_name = request.json.get('host_name', settings.default_host)
    done = False
    try:
        if not request.json:
            abort(400)
        host = request.json.get('host', settings.default_host)
        port = request.json.get('port', settings.default_port)
        cpus = request.json.get('cpus', settings.default_cpus_host)
        memory = request.json.get('memory', settings.default_memory_host)
        disk = request.json.get('disk', settings.default_disk_host)
        data_list = {}
        data_list['host'] = host
        data_list['port'] = port
        data_list['cpus'] = cpus
        data_list['memory'] = memory
        data_list['disk'] = disk
        data_list['containers'] = []
        data = json.dumps(data_list)
        etcd_client = etcd.Client(host=settings.etcd_host, port=settings.etcd_port)
        etcd_client.write(settings.etcd_prefix + settings.etcd_hosts_prefix + host_name, data)
        done = True
        logging.debug('host ' + host_name + ' added')
        return 'OK'
    except Exception as e:
        logging.error(e, exc_info=True)
        pass


@app.route('/add_host', methods=['GET'])
def add_host():
    try:
        logging.debug('add_host')
        done = False
        host_name = request.args.get('host_name', settings.default_host)
        host = request.args.get('host', settings.default_host)
        port = request.args.get('port', settings.default_port)
        cpus = request.args.get('cpus', settings.default_cpus_host)
        memory = request.args.get('memory', settings.default_memory_host)
        disk = request.args.get('disk', settings.default_disk_host)
        data_list = {}
        data_list['host'] = host
        data_list['port'] = port
        data_list['cpus'] = cpus
        data_list['memory'] = memory
        data_list['disk'] = disk
        data_list['containers'] = []
        data = json.dumps(data_list)
        etcd_client = etcd.Client(host=settings.etcd_host, port=settings.etcd_port)
        etcd_client.write(settings.etcd_prefix + settings.etcd_hosts_prefix + host_name, data)
        done = True
        logging.debug('host ' + host_name + ' added')
        return render_template('add_host.html', done=done)
    except Exception as e:
        logging.error(e, exc_info=True)
        pass


@app.route('/add_pod', methods=['GET'])
def add_pod():
    try:
        logging.debug('add_pod')
        done = False
        pod_name = request.args.get('name', settings.default_pod_name)
        if (pod_name != ''):
            image = request.args.get('image', settings.default_image_pod)
            containers = request.args.get('containers', 1)
            cpus = request.args.get('cpus', settings.default_cpus_pod)
            memory = request.args.get('memory', settings.default_memory_pod)
            disk = request.args.get('disk', settings.default_disk_pod)
            data_list = {}
            data_list['image'] = image
            data_list['containers'] = containers
            data_list['cpus'] = cpus
            data_list['memory'] = memory
            data_list['disk'] = disk
            data_list['state'] = 'stopped'
            data_list['running_containers'] = 0
            data_list['containers_list'] = []
            data = json.dumps(data_list)
            etcd_client = etcd.Client(host=settings.etcd_host, port=settings.etcd_port)
            etcd_client.write(settings.etcd_prefix + settings.etcd_pods_prefix + pod_name, data)
            done = True
            logging.debug('pod ' + pod_name + ' added')
        return render_template('add_pod.html', done=done)
    except Exception as e:
        logging.error(e, exc_info=True)
        pass


@app.route('/add_pod', methods=['POST'])
def add_pod_post():
    try:
        logging.debug('add_pod')
        done = False
        pod_name = request.json.get('name', settings.default_host)
        if not request.json:
            abort(400)
        image = request.json.get('image', settings.default_image_pod)
        containers = request.json.get('containers', 1)
        cpus = request.json.get('cpus', settings.default_cpus_pod)
        memory = request.json.get('memory', settings.default_memory_pod)
        disk = request.json.get('disk', settings.default_disk_pod)
        data_list = {}
        data_list['image'] = image
        data_list['containers'] = containers
        data_list['cpus'] = cpus
        data_list['memory'] = memory
        data_list['disk'] = disk
        data_list['state'] = 'stopped'
        data_list['running_containers'] = 0
        data_list['containers_list'] = []
        data = json.dumps(data_list)
        etcd_client = etcd.Client(host=settings.etcd_host, port=settings.etcd_port)
        etcd_client.write(settings.etcd_prefix + settings.etcd_pods_prefix + pod_name, data)
        done = True
        logging.debug('pod ' + pod_name + ' added')
        return render_template('add_pod.html', done=done)
    except Exception as e:
        logging.error(e, exc_info=True)
        pass

@app.route("/", methods=['GET'])
def index():
    try:
        logging.debug('status')
        hosts_total = -1
        pods_total = -1
        hosts_list = []
        pods_list = []
        print_hosts = False
        print_pods = False
        etcd_client = etcd.Client(host=settings.etcd_host, port=settings.etcd_port)
        hosts_ls = etcd_client.get(settings.etcd_prefix + settings.etcd_hosts_prefix)
        pods_ls = etcd_client.get(settings.etcd_prefix + settings.etcd_pods_prefix)
        for host_ls in hosts_ls.children:
            hosts_total += 1
        for pod_ls in pods_ls.children:
            pods_total += 1
        if hosts_total > 0:
            print_hosts = True
            for host_ls in hosts_ls.children:
                hosts_list += host_ls.key + ':' + host_ls.value
        if pods_total > 0:
            print_pods = True
            for pod_ls in pods_ls.children:
                pods_list += pod_ls.key + ':' + pod_ls.value
        return render_template("index.html", hosts_total=hosts_total, pods_total=pods_total,
                               print_hosts=print_hosts, print_pods=print_pods, hosts_list=hosts_list,
                               pods_list=pods_list)
    except Exception as e:
        logging.error(e, exc_info=True)
        pass


if __name__ == "__main__":
    app.run(host='::', debug=True)
