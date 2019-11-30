#  Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Amazon Software License (the "License"). You may not use
#  this file except in compliance with the License. A copy of the License is
#  located at:
#
#    http://aws.amazon.com/asl/
#
#  or in the "license" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.

import hashlib
from datetime import datetime

from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext

import utils


class Resource(object):
    def __init__(self, database, table_name,  kind, catalog_id=None):
        self.database = database
        self.table_name = table_name
        self.catalog_id = catalog_id
        self.kind = kind


class Source(Resource):
    def __init__(self, database, table_name, source_kind, catalog_id):
        Resource.__init__(self, database, table_name, "SOURCE", catalog_id)
        self.source_kind = source_kind


class JDBCSource(Source):
    def __init__(self, database, table_name, catalog_id, source_prefix):
        Source.__init__(self, database, table_name, "JDBC", catalog_id)
        self.source_prefix = source_prefix


class Target(Resource):
    def __init__(self, database, table_name, catalog_id, S3_location, target_prefix, output_format,
                 job_name, job_run_id, table_config, temp_prefix):

        Resource.__init__(self, database, target_prefix + table_name, "TARGET", catalog_id)
        self.S3_location = S3_location
        self.target_prefix = target_prefix
        self.output_format = output_format
        self.job_name = job_name
        self.job_run_id = job_run_id
        self.table_config = table_config
        self.temp_prefix = temp_prefix
        # all prefixes already have a trailing '_'
        self.temp_table_name = "{}{}{}".format(temp_prefix, target_prefix, table_name)


class Transform(object):
    def __init__(self, glue_client, source, target, glue_context, target_location):
        self.glue_client = glue_client
        self.source = source
        self.target = target
        self.table_exists = False
        self.glue_context = glue_context
        self.target_location = target_location
        self.source_schema = self.glue_client.get_table(CatalogId=self.source.catalog_id, DatabaseName=self.source.database, Name=self.source.table_name)['Table']['StorageDescriptor']['Columns']

    def get_schema(self, partition_spec):
        return [{"Name": i['Name'], "Type": i['Type']} for i in self.source_schema if i['Name'] not in partition_spec]

    def get_mappings(self):
        return [(i['Name'], i['Name'], i['Type']) for i in self.source_schema]

    def _snapshot_transform(self):
        datasource0 = self.glue_context.create_dynamic_frame.from_catalog(database=self.source.database,
                                                                          catalog_id=self.source.catalog_id,
                                                                          table_name=self.source.table_name)

        applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=self.get_mappings())

        dropnullfields2 = DropNullFields.apply(frame=applymapping1)

        datasink3 = self.glue_context.write_dynamic_frame.from_catalog(frame=dropnullfields2,
                                                                       catalog_id=self.target.catalog_id,
                                                                       database=self.target.database,
                                                                       table_name=self.target.temp_table_name)

    def transform(self):
        self._snapshot_transform()


class Driver(object):
    def __init__(self):

        args = utils.get_job_args(['JOB_NAME',
                                   'JOB_RUN_ID',
                                   'WORKFLOW_NAME',
                                   'WORKFLOW_RUN_ID',
                                   'region',
                                   'source_table_prefix',
                                   'glue_endpoint',
                                   'catalog_id',
                                   'target_s3_location',
                                   'target_database',
                                   'target_format',
                                   'job_index',
                                   'num_jobs'],
                                  ['target_table_prefix',
                                  'creator_arn',
                                   'version'])

        self.glue_endpoint = args['glue_endpoint']
        self.region = args['region']
        self.job_name = args['JOB_NAME']
        self.job_run_id = args['JOB_RUN_ID']
        self.workflow_name = args['WORKFLOW_NAME']
        self.workflow_run_id = args['WORKFLOW_RUN_ID']

        self.creator_arn = args['creator_arn']

        self.glue_client = utils.build_glue_client(self.region, self.glue_endpoint)
        self.lf_client = utils.build_lakeformation_client(self.region, self.glue_endpoint)

        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc, minPartitions=1, targetPartitions=1)

        self.catalog_id = args['catalog_id']
        self.target_database = args['target_database']
        self.source_prefix = args['source_table_prefix']
        self.target_prefix = args['target_table_prefix']
        self.target_s3_location = args['target_s3_location']
        self.output_format = args['target_format']

        self.version = args['version']

        if self.version and int(self.version) > 0:
            self.temp_prefix = "_temp_"
        else:
            self.temp_prefix = "temp_"

        self.tables = self.get_tables_config(int(args['job_index']), int(args['num_jobs']))

    def table_exists(self, table_name):
        try:
            self.glue_client.get_table(CatalogId=self.catalog_id, DatabaseName=self.target_database,
                                       Name=table_name)
            return True
        except:
            return False

    def should_process_table(self, job_index, num_jobs):
        def h(table_config):
            catalog_table_name = table_config['catalog_table_name']
            md5 = int(hashlib.md5(catalog_table_name).hexdigest(), 16)
            return (md5 % num_jobs) == job_index

        return h

    def get_storage_descriptor(self, format, schema, table_name):
        if format.lower() == "parquet":
            input_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            output_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            serde_info = {'Parameters': {'serialization.format': '1'},
                          'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'}
        elif format.lower() == "csv":
            input_format = "org.apache.hadoop.mapred.TextInputFormat"
            output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            serde_info = {'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                          'Parameters': {'field.delim': ','}}
        elif format.lower() == "json":
            input_format = ""
            output_format = ""
            serde_info = {}
        else:
            raise Exception("Format should be CSV or Parquet, but got: {}".format(format))

        return {'Columns': schema, 'InputFormat': input_format, 'OutputFormat': output_format, 'SerdeInfo': serde_info,
                'Location': self.target_s3_location + table_name + "/"}

    def create_table(self, source, target, transform):
        #TODO: porwalv comment out JDBC partitioning
        #schema = transform.get_schema(source.partition_spec)
        schema = transform.source_schema
        table_input = {'Name': target.table_name,
                       'StorageDescriptor' : self.get_storage_descriptor(target.output_format, schema, target.table_name),
                       'Parameters': {'classification': target.output_format,
                                      'SourceType': source.source_kind,
                                      'SourceTableName': source.table_name[len(source.source_prefix):],
                                      'CreatedByJob': target.job_name,
                                      'CreatedByJobRun': target.job_run_id,
                                      'TableVersion': "0"}
                       }

        get_table_result = self.glue_client.get_table(CatalogId=source.catalog_id, DatabaseName=self.target_database, Name=source.table_name)['Table']
        table_input['Parameters']['SourceConnection'] = get_table_result['Parameters']['connectionName']

        if target.output_format.lower() == "csv":
            table_input['Parameters']['skip.header.line.count'] = "1"

        self.glue_client.create_table(CatalogId=target.catalog_id, DatabaseName=target.database, TableInput=table_input)

        table_input['Name'] = target.temp_table_name
        table_input['StorageDescriptor']['Location'] += "version_0/"
        self.glue_client.create_table(CatalogId=target.catalog_id, DatabaseName=target.database, TableInput=table_input)

        target.storage_descriptor = self.get_storage_descriptor(target.output_format, schema, source.table_name)

    def update_table(self, src, tgt):
        source = self.glue_client.get_table(CatalogId=src.catalog_id, DatabaseName=src.database, Name=src.table_name)['Table']
        target = self.glue_client.get_table(CatalogId=tgt.catalog_id, DatabaseName=tgt.database, Name=tgt.table_name)['Table']

        source_schema = source['StorageDescriptor']['Columns']
        table_input = {key: target[key] for key in target if key not in ['CreatedBy', 'CreateTime', 'UpdateTime', 'DatabaseName']}
        table_input['StorageDescriptor']['Columns'] = source_schema

        #prev_version = "0" if ('Parameters' not in table_input or 'TableVersion' not in table_input['Parameters']) else table_input['Parameters']['TableVersion']
        prev_version = table_input['Parameters']['TableVersion']
        new_version = str(int(prev_version) + 1)
        table_input['Parameters']['TableVersion'] = new_version
        new_location = table_input['StorageDescriptor']['Location'][:-len("version_" + prev_version + "/")]
        table_input['StorageDescriptor']['Location'] = new_location + "version_" + new_version + "/"
        table_input['Name'] = tgt.temp_table_name

        print("[INFO] Trying to update: Source table: {}, TargetTable: {}".format(src.table_name, table_input['Name']))
        self.glue_client.update_table(CatalogId=tgt.catalog_id, DatabaseName=tgt.database, TableInput=table_input)

    def hot_swap_tables(self, source, target, delta, end_time):
        temp = self.glue_client.get_table(CatalogId=target.catalog_id, DatabaseName=target.database, Name=target.temp_table_name)['Table']
        tgt = self.glue_client.get_table(CatalogId=target.catalog_id, DatabaseName=target.database, Name=target.table_name)['Table']

        table_input = {key: tgt[key] for key in tgt if key not in ['CreatedBy', 'CreateTime', 'UpdateTime', 'DatabaseName']}

        table_input['StorageDescriptor']['Columns'] = temp['StorageDescriptor']['Columns']
        table_input['StorageDescriptor']['Location'] = temp['StorageDescriptor']['Location']
        table_input['Parameters']['TableVersion'] = temp['Parameters']['TableVersion']
        table_input['Parameters']['LastUpdatedByJob'] = target.job_name
        table_input['Parameters']['LastUpdatedByJobRun'] = target.job_run_id
        table_input['Parameters']['TransformTime'] = delta
        table_input['Parameters']['LastTransformCompletedOn'] = end_time

        self.glue_client.update_table(CatalogId=target.catalog_id, DatabaseName=target.database, TableInput=table_input)

    def update_table_job_info(self, source, target, delta, end_time):
        source = self.glue_client.get_table(CatalogId=target.catalog_id, DatabaseName=target.database, Name=source.table_name)['Table']

        table_input = {key: source[key] for key in source if key not in ['CreatedBy', 'CreateTime', 'UpdateTime', 'DatabaseName']}
        table_input['Parameters']['LastUpdatedByJob'] = target.job_name
        table_input['Parameters']['LastUpdatedByJobRun'] = target.job_run_id
        table_input['Parameters']['TransformTime'] = delta
        table_input['Parameters']['LastTransformCompletedOn'] = end_time

        self.glue_client.update_table(CatalogId=target.catalog_id, DatabaseName=target.database, TableInput=table_input)

    def get_tables_config(self, job_index, num_jobs):
        table_list = []
        next_token = ''
        while (True):
            get_tables_result = self.glue_client.get_tables(DatabaseName=self.target_database,
                                                            CatalogId=self.catalog_id,
                                                            Expression="^{}.*".format(self.source_prefix),
                                                            NextToken=next_token)
            next_token = get_tables_result.get('NextToken')
            table_list.extend([t['Name'] for t in get_tables_result['TableList']])
            if not next_token:
                break

        result = [{'catalog_table_name': i, "sourceType": "JDBC"} for i in table_list]

        selected_tables = filter(self.should_process_table(job_index, num_jobs), result)
        print("[INFO]: Will process {} tables: {}".format(len(selected_tables),
                                                          [config['catalog_table_name'] for config in selected_tables]))

        return selected_tables

    def run_transform(self):
        for i in self.tables:
            if i.get("sourceType") == "JDBC":
                source = JDBCSource(self.target_database, i.get('catalog_table_name'), self.catalog_id, self.source_prefix)

            original_table_name = i.get('catalog_table_name')[len(source.source_prefix):]
            target = Target(self.target_database, original_table_name , self.catalog_id, self.target_s3_location,
                            self.target_prefix, self.output_format, self.job_name, self.job_run_id, i, self.temp_prefix)
            transform = Transform(self.glue_client, source, target, self.glue_context, self.target_s3_location)

            print("[INFO] Source table name: {}, target table name: {}".format(source.table_name, target.table_name))
            if not self.table_exists(target.table_name):
                print("[INFO] Target Table name: {} does not exist.".format(target.table_name))
                self.create_table(source, target, transform)
                target_table_already_exists = False
            else:
                print("[INFO] Target Table name: {} exist.".format(target.table_name))
                self.update_table(source, target)
                target_table_already_exists = True

            start_time = datetime.now()
            transform.transform()
            end_time = datetime.now()

            self.hot_swap_tables(source, target, str(end_time - start_time), str(end_time))

            # Give permissions to the table if we are just creating it for the first time
            if not target_table_already_exists:
                utils.grant_all_permission_to_creator(self.lf_client,
                                                      target.catalog_id,
                                                      target.database,
                                                      target.table_name,
                                                      self.creator_arn)
            else:
                print("[INFO] Not setting up permissions for the creator on the ingested table")



def main():
    driver = Driver()
    driver.run_transform()


if __name__ == "__main__":
    main()
