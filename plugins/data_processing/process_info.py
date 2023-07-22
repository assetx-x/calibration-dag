import os
import docker
import psutil
import requests

class ProcessInfo(object):
    info_fields = {
        "process_fields": ['pid', 'username', 'create_time', 'cpu_percent', 'cpu_times', 'memory_info', 'num_threads',
                           'exe', 'memory_percent'],
        "container_fields": ['container_pid', 'container_start_time', 'container_image', 'container_hostname',
                             'container_args', 'container_hostnamepath', 'container_name', 'container_created_dt',
                             'container_network_id', 'container_endpoint_id', 'container_sandbox_id',
                             'container_mac_address', 'container_gateway', 'container_ip_address',
                             'container_image_sha', 'container_kernel_memory', 'container_cpu_percent',
                             'container_memory', 'container_disk_quota', 'container_cpu_shares', 'container_cpu_count',
                             'container_id', 'container_host_external_ip']
    }

    @staticmethod
    def get_process_info():
        all_info = psutil.Process(os.getpid()).as_dict()
        process_info = {k: str(all_info[k]) for k in ProcessInfo.info_fields['process_fields']}
        return process_info

    @staticmethod
    def get_container_info():
        container_info = {}
        try:
            cli = docker.client.from_env(version='auto')
            proc_data = read_csv("/proc/self/cgroup", sep="^").iloc[:,0].str.split(":").str[2].str.split("/").str[2]
            container_id = proc_data[proc_data.str.startswith("docker")].iloc[0][len("docker")+1:-len("scope")-1]
            all_info = cli.containers.get(container_id).attrs
            container_info['container_pid'] = str(all_info['State']['Pid'])
            container_info['container_start_time'] = str(all_info['State']['StartedAt'])
            container_info['container_image'] = str(all_info['Config']['Image'])
            container_info['container_hostname'] = str(all_info['Config']['Hostname'])
            container_info['container_args'] = str(all_info['Args'])
            container_info['container_hostnamepath'] = str(all_info['HostnamePath'])
            container_info['container_name'] = str(all_info['Name'])
            container_info['container_created_dt'] = str(all_info['Created'])
            container_info['container_sandbox_id'] = str(all_info['NetworkSettings']['SandboxID'])
            container_info['container_mac_address'] = str(all_info['NetworkSettings']['MacAddress'])
            container_info['container_gateway'] = str(all_info['NetworkSettings']['Gateway'])
            container_info['container_ip_address'] = str(all_info['NetworkSettings']['IPAddress'])
            container_info['container_image_sha'] = str(all_info['Image'])
            container_info['container_kernel_memory'] = str(all_info['HostConfig']['KernelMemory'])
            container_info['container_memory'] = str(all_info['HostConfig']['Memory'])
            container_info['container_id'] = str(all_info['Id'])
            container_info["container_host_external_ip"] = requests.get("http://169.254.169.254/latest/meta-data/local-ipv4").text

            network_info_key = all_info['NetworkSettings']['Networks'].keys()[0]
            container_info['container_network_id'] = str(all_info['NetworkSettings']['Networks'][network_info_key]['NetworkID'])
            container_info['container_endpoint_id'] = str(all_info['NetworkSettings']['Networks'][network_info_key]['EndpointID'])

            container_info['container_cpu_percent'] = str(all_info['HostConfig']['CpuPercent'])
            container_info['container_disk_quota'] = str(all_info['HostConfig']['DiskQuota'])
            container_info['container_cpu_shares'] = str(all_info['HostConfig']['CpuShares'])
            container_info['container_cpu_count'] = str(all_info['HostConfig']['CpuCount'])
        except BaseException:
            pass
        return container_info

    @staticmethod
    def retrieve_all_process_info():
        info_fields = list(ProcessInfo.info_fields['process_fields'])
        info_fields.extend(ProcessInfo.info_fields['container_fields'])
        info = dict(zip(info_fields, [None]*len(info_fields)))
        # get process info here
        process_info = ProcessInfo.get_process_info()
        info.update(process_info)
        container_info = ProcessInfo.get_container_info()
        if len(container_info):
            info.update(container_info)
        return info