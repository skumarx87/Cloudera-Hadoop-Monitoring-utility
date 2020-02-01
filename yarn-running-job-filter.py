import json, urllib3
import argparse
# Passing the rest api url of the resource manager and filtering the applications to fetch the running ones
class yarn_idle_sort:
        def __init__(self,hours,rm):
                self.hours_filter = hours
                self.rm_rest_api = rm
                self.http = urllib3.PoolManager()
                self.threshold = self.hours_filter * 3600000
        def yarn_filter_by_elapsedtime(self):
                response=self.http.request('GET', self.rm_rest_api)
                data=json.loads(response.data)
                print ("Please find the list of long running jobs.")
                for running_apps in data['apps']['app']:
                        if running_apps['elapsedTime']>self.threshold:
                                print ("\nApp Name: {}".format(running_apps['name']))
                                print ("Application id: {}".format(running_apps['id']))
                                print ("Total elapsed time: {} hours".format(round(running_apps['elapsedTime']/1000/60/60)))
                                print ("queue Name: {}".format(running_apps['queue']))
                                print ("Allocated memory: {} gb".format(running_apps['allocatedMB']/1024))
                                print ("Tracking Url: ",running_apps['trackingUrl'])
                        else:
                                print ("No long running jobs!")



if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Yarn idle resources filter.')
        parser.add_argument('-t','--hours',type=int, default = 1,help='provide elapsed hours to filter')
        args = parser.parse_args()
        rm="http://yarn-resourcmanagerurl:8088/ws/v1/cluster/apps?states=RUNNING"
        obj = yarn_idle_sort(args.hours,rm)
        obj.yarn_filter_by_elapsedtime()
