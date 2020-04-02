# This is the script to get the list of applications which are running for more than N hours

from __future__ import division
import smtplib
from email.mime.text import MIMEText
import json, urllib3
import argparse
import subprocess
import datetime
# Passing the rest api url of the resource manager and filtering the applications to fetch the running ones
class yarn_pool_util:
	def __init__(self,rm):
		self.rm_rest_api = rm
		self.http = urllib3.PoolManager()
		#self.threshold = self.hours_filter * 3600000 
		self.mail_output = True 
	def yarn_utilization(self):
		mail_output = open('/tmp/mail_output.log','w')
		now = datetime.datetime.now()
		response=self.http.request('GET', self.rm_rest_api)
		data=json.loads(response.data)
		#print(data)
		root_queue=data['scheduler']['schedulerInfo']['rootQueue']
		child_queues=root_queue['childQueues']
		t_a_vcore=root_queue['maxResources']['vCores']
		t_a_memory=root_queue['maxResources']['memory']
		t_u_vcore=root_queue['usedResources']['vCores']
		t_u_memory=root_queue['usedResources']['memory']
		#print("Total Resource availabe :{}".format(root_queue['steadyFairResources']))
		#print("Max used  Resource :{}".format(root_queue['usedResources']))
		i=0
		#print("Queue Name:Cpu allocation:MemoryAllocation")
		print('<table>')
		print('<tr></tr>')
		print('<tr><td>Report generation time:</td><td>{}</td></tr>'.format(now))
		print('<tr><td>Total Resource availabe:</td><td>{} Vcores,</td><td>{}TB</td></tr>'.format(t_a_vcore,round(t_a_memory/1048576,2)))
		print('<tr><td>Total Resource Utilized(value):</td><td>{} Vcores,</td><td>{}TB</td></tr>'.format(t_u_vcore,round(t_u_memory/1048576,2)))
		print('<tr><td>Total Resource Utilized(%):</td><td>{} %,</td><td>{}%</td></tr>'.format(round((t_u_vcore/t_a_vcore)*100,2),round((t_u_memory/t_a_memory)*100,2)))
		print('<tr></tr>')
		print('<tr><td>Queue Name</td><td>Max Allocation(cpu)</td><td>Max Allocation(memory)</td>')
		print('<td>Cpu Utilizied(%)</td><td>Memory Utilized(%)</td></tr>')
                mail_output.write('<table>')
                mail_output.write('<tr></tr>')
                mail_output.write('<tr><td>Report generation time:</td><td>{}</td></tr>'.format(now))
                mail_output.write('<tr><td>Total Resource availabe:</td><td>{} Vcores,</td><td>{}TB</td></tr>'.format(t_a_vcore,round(t_a_memory/1048576,2)))
                mail_output.write('<tr><td>Total Resource Utilized(value):</td><td>{} Vcores,</td><td>{}TB</td></tr>'.format(t_u_vcore,round(t_u_memory/1048576,2)))
                mail_output.write('<tr><td>Total Resource Utilized(%):</td><td>{} %,</td><td>{}%</td></tr>'.format(round((t_u_vcore/t_a_vcore)*100,2),round((t_u_memory/t_a_memory)*100,2)))
                mail_output.write('<tr></tr>')
                mail_output.write('<tr><td>Queue Name</td><td>Max Allocation(cpu)</td><td>Max Allocation(memory)</td>')
                mail_output.write('<td>Cpu Utilizied(%)</td><td>Memory Utilized(%)</td></tr>')

		while i < len(child_queues['queue']):
			#print(i)
			#print(child_queues['queue'][i])
			quername=child_queues['queue'][i]['queueName']
			#print("Total core {} used core {}".format(t_vcore,child_queues['queue'][i]['maxResources']['vCores']))
			m_v_core=child_queues['queue'][i]['maxResources']['vCores']
			m_meory=child_queues['queue'][i]['maxResources']['memory']
			u_v_core=child_queues['queue'][i]['usedResources']['vCores']
			u_meory=child_queues['queue'][i]['usedResources']['memory']
			p_m_vcore=round((m_v_core/t_a_vcore)*100)
			p_m_memory=round((m_meory/t_a_memory)*100)
                        p_u_vcore=round((u_v_core/t_a_vcore)*100)
                        p_u_memory=round((u_meory/t_a_memory)*100)
			print("<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(quername,p_m_vcore,p_m_memory,p_u_vcore,p_u_memory))
			mail_output.write("<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(quername,p_m_vcore,p_m_memory,p_u_vcore,p_u_memory))
			i = i+1
		print('</table>')
		mail_output.write('</table>')

	def send_mail_to_admins(self):
		if self.mail_output:
			with open('/tmp/mail_output.log') as fp:
				msg = MIMEText(fp.read(),'html')
			server = smtplib.SMTP('smtp.tanu.com')
			sender = "no_reply@tanu.com"
			recipients = ['admins@tanu.com']
			msg['Subject'] = "Prod: Yarn utilization report"
			msg['From'] = sender
			msg['To'] = ", ".join(recipients)
			server.sendmail(sender,recipients,msg.as_string())
			server.quit()
		 

if __name__ == '__main__':
	rm="http://yarnrm.tanu.com:8088/ws/v1/cluster/scheduler?openQueues=root"
	obj = yarn_pool_util(rm)
	obj.yarn_utilization()
	obj.send_mail_to_admins()
