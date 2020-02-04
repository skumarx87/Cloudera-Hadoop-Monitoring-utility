
import json, urllib3
import sys
import json
from prettytable import PrettyTable
from beautifultable import BeautifulTable

class kudu_api:
        def __init__(self):
                #self.server=server
                #self.tablet_api = 'http://'+self.server+'/metrics'
                self.http = urllib3.PoolManager()
                self.tablets = []
                self.tablets_counts = {}
                self.t = PrettyTable(['Server', 'Running tablets','Total_tablets'])
                self.table = BeautifulTable()
                self.table.column_headers = ['Server', 'Running tablets','Total_tablets']
        def read_metric(self,server):
                tablet_api = 'http://'+server+'/metrics'
                tablets = []
                tablets_counts ={}
                response = None
                print(tablet_api)
                try:
                        response=self.http.request('GET', tablet_api)
                except Exception:
                        print("Error")
                        pass
                if response is not None:
                        data=json.loads(response.data.decode('utf-8'))
                        for i in range(len(data)):
                                if data[i]['type'] == 'tablet':
                                        tablets.append(data[i]['id'])
                                if data[i]['type'] == 'server':
                                        for y in range(7):
                                                tablets_counts[data[i]['metrics'][y]['name']] = data[i]['metrics'][y]['value']



                #print(len(tablets))
                print((sum(tablets_counts.values()) - len(tablets)))
                #t.add_row(self.server,len(self.tablets),sum(self.tablets_counts.values()))
                self.table.append_row([server,len(tablets),(sum(tablets_counts.values()) - len(tablets))])
        def print_table(self):
                print(self.table)



if __name__ == '__main__':

        obj = kudu_api()
        with open('prod_tablet') as f:
        #with open('tablet_server') as f:
                for server in f:
                        #obj = kudu_api()
                        obj.read_metric(server.strip())
                obj.print_table()

