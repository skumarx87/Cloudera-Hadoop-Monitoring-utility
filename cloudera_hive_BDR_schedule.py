import sys
import time
from cm_api.api_client import ApiResource

from cm_api.api_client import ApiResource from cm_api.endpoints.types import *


## ** Settings to connect to Backup cluster ** class cloudera_bdr:
        def __init__(self,cm,port,uname,passwd,bkf_cluster_name):
                self.backup_cluster_name = bkf_cluster_name
                self.api = ApiResource(server_host=cm,server_port=port,username=uname,password=passwd)
                self.peer=self.api.get_cloudera_manager().get_peer('PROD')
                clusters = self.api.get_all_clusters()
                #self.backup_cluster = None
                for cluster in clusters:
                        if cluster.displayName == self.backup_cluster_name.strip():
                                self.backup_cluster = cluster
                                break
                        if backup_cluster is None:
                                print "\nError: Cluster '" + self.backup_cluster_name + "' not found"

        def create_hive_schedule(self,database_list):
                ## Get Hive Service
                hive_service = None
                service_list = self.backup_cluster.get_all_services()
                for service in service_list:
                        if service.type == "HIVE":
                                hive_service = service
                                break
                if hive_service is None:
                        print "Error: Could not locate Hive Service"
                        quit(1)
                hive_args = ApiHiveReplicationArguments(None)
                hdfs_args = ApiHdfsReplicationArguments(None)
                hdfs_args.preserveBlockSize=True
                hdfs_args.preservePermissions=True
                hdfs_args.mapreduceServiceName='Yarn'
                hive_args.sourceService = ApiServiceRef(None,peerName=self.peer.name,clusterName='cluster',serviceName='hive')
                hive_args.hdfsArguments=hdfs_args
                tablesf = []
                for db in database_list:
                        tablesf.append(ApiHiveTable(None,database=db,tableName='[\w_]+'))
                hive_args.tableFilters=tablesf
                hive_args.force=True
                print(database_list)
                schedule = hive_service.create_replication_schedule(start_time=None, end_time=None,interval_unit='MINUTE', interval=0,paused=False,arguments=hive_args)
                print(schedule.id)

        def split_hive_schedule(self,filepath):
                with open(filepath) as f:
                        db_list = []
                        for db in f:
                                db_list.append(db.strip())
                #print(db_list)
                n = 10
                final_list = [db_list[i * n:(i + 1) * n] for i in range((len(db_list) + n - 1) // n )]
                for list in final_list:
                        self.create_hive_schedule(list)

if __name__ == '__main__':
        cm_host = "dr-mn.tanu.com"
        cm_port = "7180"
        cm_login = "admin"
        cm_password = "admin"
        backup_cluster_name = "CLOUDERA-DR"
        obj = cloudera_bdr(cm_host,cm_port,cm_login,cm_password,backup_cluster_name)
        obj.split_hive_schedule('/tmp/hivedblist.txt)

