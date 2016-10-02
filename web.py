from flask import Flask, request, render_template
import json
import settings
import etcd

app = Flask(__name__)
etcd_client = etcd.Client(host=settings.etcd_host, port=settings.etcd_port)


@app.route('/add_slave')
def add_slave():
    pass


@app.route('/add_pod')
def add_pod():
    done=False
    pod_name = request.args.get('pod_name', '')
    if (pod_name != ''):
        containers_number = request.args.get('containers_number', 1)
        cpu_count = request.args.get('cpu_count', 1)
        memory_mb = request.args.get('memory_mb', 200)
        disk_gb = request.args.get('disk_gb', 2)
        data_list = {}
        data_list['count'] = containers_number
        data_list['cpu'] = cpu_count
        data_list['memory'] = memory_mb
        data_list['disk'] = disk_gb
        data = json.dumps(data_list)
        etcd_client.write(settings.etcd_prefix + 'pods/' + pod_name, data)
        done=True
    return render_template('add_pod.html',done=done)


@app.route("/", methods=['GET'])
def hello():
    slaves_total = -1
    pods_total = -1
    slaves_list = []
    pods_list = []
    print_slaves = False
    print_pods = True
    slaves_ls = etcd_client.get(settings.etcd_prefix + 'slaves')
    pods_ls = etcd_client.get(settings.etcd_prefix + 'pods')
    for slave_ls in slaves_ls.children:
        slaves_total += 1
    for pod_ls in pods_ls.children:
        pods_total += 1
    if slaves_total > 0:
        print_slaves = True
        for slave_ls in slaves_ls.children:
            slaves_list += slave_ls.key + ':' + slave_ls.value
    if pods_total > 0:
        print_pods = True
        for pod_ls in pods_ls.children:
            pods_list += pod_ls.key + ':' + pod_ls.value
    return render_template("index.html", slaves_total=slaves_total, pods_total=pods_total,
                           print_slaves=print_slaves, print_pods=print_pods, slaves_list=slaves_list,
                           pods_list=pods_list)


if __name__ == "__main__":
    app.run(host='::', debug=True)
