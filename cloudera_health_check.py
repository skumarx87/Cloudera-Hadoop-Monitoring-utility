import cm_client
from cm_client.rest import ApiException
from pprint import pprint
import smtplib
from email.mime.text import MIMEText

class cloudera_monitoring:
        def __init__(self,cm_host,env,send_alert):
                cm_client.configuration.username = 'admin'
                cm_client.configuration.password = 'admin'
                self.env = env
                api_host = 'http://'+cm_host
                port = '7180'
                api_version = 'v19'
                api_url = api_host + ':' + port + '/api/' + api_version
                self.send_alert_email = send_alert
                print(api_url)
                self.api_client = cm_client.ApiClient(api_url)
                self.cluster_api_instance = cm_client.ClustersResourceApi(self.api_client)
                self.services_api_instance = cm_client.ServicesResourceApi(self.api_client)
                self.roles_api_instance = cm_client.RolesResourceApi(self.api_client)
                self.host_api_instance = cm_client.HostsResourceApi(self.api_client)
                self.host_details = self.get_hostname_by_id()
                self.bad_alert_count = 0
                self.msg_body = ""
                #print(self_host_details)
        def get_clusters(self):
                api_response = self.cluster_api_instance.read_clusters(view='SUMMARY')
                clusters = []
                for cluster in api_response.items:
                        clusters.append(cluster)
                        #print(cluster.name,cluster.full_version)
                return clusters
        def get_service(self,cluster):
                services = self.services_api_instance.read_services(cluster, view='FULL')
                cluster_services = []
                for service in services.items:
                        cluster_services.append(service)
                return cluster_services
        #def inspect_service(self,service_name):
        #       for health_check in service_name.health_checks:
        #               #print(service_name)
        #               print health_check.name, "---", health_check.summary
        def get_hostname_by_id(self):
                hosts = self.host_api_instance.read_hosts(view='summary')
                #print(hosts)
                hosts_details = {}
                for host in hosts.items:
                        hosts_details[host.host_id] = host.hostname
                        #print(host.host_id)
                return hosts_details
        def inspect_roles(self,cluster,service):
                api_instance = cm_client.ServicesResourceApi(self.api_client)
                roles = self.roles_api_instance.read_roles(cluster.display_name, service.name)
                for role in roles.items:

                        if role.health_summary == 'BAD':
                                self.bad_alert_count += 1
                                hostname = self.host_details[role.host_ref.host_id]
                                msg ="Role name: {}\nState: {}\nHealth: {}\n".format(role.name, role.role_state, role.health_summary, hostname)
                                self.msg_body +="Role name: {}</br>State: {}</br>Health: {}</br>".format(role.name, role.role_state, role.health_summary, hostname)
                                print(self.msg_body)
                                #print "Role name: %s\nState: %s\nHealth: %s\nHost: %s" % (role.name, role.role_state, role.health_summary, role.host_ref.host_id)
                                #print("Role name: %s\nState: %s\nHealth: %s\nHost: %s" % (role.name, role.role_state, role.health_summary, hostname))
                #self.send_email(bad_alert_count)
        def send_email(self):
                print("total counts : {}".format(self.bad_alert_count))
                subject = None
                sender = "test@localhost"
                server = smtplib.SMTP('localhost')
                recipients = ['hadoopsupport@tanu.com']
                if self.bad_alert_count > 1:
                        subject = "Alert : Multiple Roles are down in cloudera Cluster({}) in Env {}".format(self.bad_alert_count,self.env)
                else:
                        subject = "Alert : Cloudera cluster Role down Alert in Env {}".format(self.env)
                if self.send_alert_email:
                        msg = MIMEText(self.msg_body, 'html', 'utf-8')
                        msg['Subject'] = subject
                        msg['To'] = ", ".join(recipients)
                        server.sendmail(sender,recipients,msg.as_string())

def check_status(obj):
        clusters = obj.get_clusters()
        service_list = []
        for cluster in clusters:
                service_list = obj.get_service(cluster.name)
                for service in service_list:
                        obj.inspect_roles(cluster,service)
                obj.send_email()
if __name__ == '__main__':
        pro_obj = cloudera_monitoring('prdmn.tanu.com','PROD',True)
        sit_obj = cloudera_monitoring('devmn.tanu.com','SIT',True)
        check_status(sit_obj)
        check_status(pro_obj)
