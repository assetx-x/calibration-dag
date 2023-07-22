# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE_MAGIC_CELL
# Automatically replaced inline charts by "no-op" charts
# %pylab inline
import matplotlib
matplotlib.use("Agg")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataikuapi
from dataikuapi import CodeRecipeCreator
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import json
import boto3
import os
import io

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
from calibrations.calibration_pipelines import TaskflowPipelineDescription, TaskflowPipelineRunner
from calibrations.core.calibration_tasks_registry import CalibrationTaskRegistry
from calibrations.common.taskflow_params import BasicTaskParameters, TaskflowPipelineRunMode
from calibrations.core.core_classes import SQLReader, S3Reader
from etl_workflow_steps import _BASE_FLOW_GLOBAL_PARAMETERS_LABEL
import inspect

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
class DataIKUProjectDeleter(object):
    def __init__(self, project_key):
        self.project_key = project_key
        self._raise_on_project_key()
        self.api_client = dataiku.api_client()
        self.project = self.api_client.get_project(project_key)

    def _raise_on_project_key(self):
        if self.project_key == dataiku.default_project_key():
            raise ValueError("The Current project is protected and cannot be deleted. you use the project key = {0}".
                     format(self.project_key))

    def delete(self, delete_project=False):
        self._delete_all_datasets()
        if delete_project:
            self._delete_project()

    def _delete_project(self):
        self._raise_on_project_key()
        self.project.delete(True)

    def _delete_all_datasets(self):
        self._raise_on_project_key()
        all_datasets = [dataset["name"] for dataset in self.project.list_datasets()]
        for dataset_name in all_datasets:
            dataset = dataikuapi.dss.dataset.DSSDataset(self.api_client, self.project_key, dataset_name)
            dataset.delete()

class RecipeBuilder(object):
    def __init__(self, project_key, klass, klass_config, task_parameters):
        self.klass = klass
        self.project_key = project_key
        self.klass_config = klass_config
        self.task_parameters = task_parameters
        self.api_client = dataiku.api_client()
        self.project = self.api_client.get_project(project_key)
        self.recipe_string = ""

    def _get_required_datasets(self):
        return CalibrationTaskRegistry.get_required_data_of(self.klass.__name__)

    def _get_produced_datasets(self):
        return list(CalibrationTaskRegistry.get_produced_data_of(self.klass.__name__))

    def build(self):
        self._build_recipe_string()
        step_required_data = self._get_required_datasets()
        result_data = self._get_produced_datasets()
        builder = CodeRecipeCreator(self.klass.__name__, "python", self.project)
        for required_data in step_required_data:
            builder = builder.with_input(required_data)
        for produced_data in result_data:
            builder = builder.with_output(produced_data)
        builder = builder.with_script(self.recipe_string)
        print(self.klass)
        builder.build()
        recipe_details = self.project.get_recipe(self.klass.__name__).get_definition_and_payload()
        recipe_details.data["recipe"]["params"]["envSelection"] = {u'envMode': u'EXPLICIT_ENV', u'envName': u'dcm-intuition'}
        self.project.get_recipe(self.klass.__name__).set_definition_and_payload(recipe_details)

    def _build_recipe_string(self):
        self._add_import_statements()
        self._add_class_code()
        self._add_dataset_inputs()
        self._add_config()
        self._add_taskflow_execution()
        self._add_dataset_outputs()


    def _add_import_statements(self):
        dataiku_imports_text = "# -*- coding: utf-8 -*- \nimport dataiku \nimport dataikuapi  \nfrom dataiku import pandasutils as pdu" \
                               + "\nfrom etl_workflow_steps import compose_main_flow_and_engine_for_task\n" + \
                               "from calibrations.common.taskflow_params import BasicTaskParameters, TaskflowPipelineRunMode\n"
        klass_source_file = inspect.getsourcefile(self.klass)
        with open(klass_source_file) as f:
            content = f.readlines()
        import_line_index = (pd.Series(content).str.startswith("from") | pd.Series(content).str.startswith("import"))[0:100].replace({False:pd.np.NaN}).last_valid_index() + 1
        all_import_lines = content[0:import_line_index]
        import_code = " \n".join(all_import_lines)
        import_code = dataiku_imports_text + import_code
        import_code = import_code.replace("\n \n","\n")
        import_code = import_code.replace("\n\n","\n")
        self.recipe_string += import_code


    def _add_class_code(self):
        klass_source_code = inspect.getsource(self.klass)
        self.recipe_string += ("\n \n \n" +klass_source_code)


    def _add_dataset_inputs(self):
        step_required_data = self._get_required_datasets()

        dataset_input_string = \
        """
step_required_data = {0}
required_data = {{}}
for ds_name in step_required_data:
    ds = dataiku.Dataset(ds_name)
    df = ds.get_dataframe()
    required_data[ds_name] = df
        """.format(json.dumps(step_required_data))
        self.recipe_string += ("\n \n \n" +dataset_input_string)

    def _add_config(self):
        # needs to add something like this, in a way it can be parsed as a proper dictionary in python when run
        self.recipe_string += "\nklass_config = {0}".format(json.loads(json.dumps(self.klass_config)))

    def _add_taskflow_execution(self):
        execution_string = \
"""
run_params = BasicTaskParameters(pd.Timestamp("{0}"), pd.Timestamp("{1}"), pd.Timestamp("{2}"),
                                 pd.Timestamp("{3}"), {4})
step_instance = {5}(**klass_config)
engine, main_flow = compose_main_flow_and_engine_for_task("test_{5}", run_params, step_instance, store=required_data)
engine.run(main_flow)
""".format(*(self.task_parameters+(self.klass.__name__,)))
        self.recipe_string += ("\n \n \n" +execution_string)

    def _add_dataset_outputs(self):
        result_data = self._get_produced_datasets()
        dataset_output_string = \
"""
result_data_names = {0}
results = {{k:engine.storage.fetch(k) for k in result_data_names}}
api_client=dataiku.api_client()
proj_key=dataiku.default_project_key()
for ds_name in results:
    df = results[ds_name]
    ds = dataikuapi.dss.dataset.DSSDataset(api_client, proj_key, ds_name)
    result_schema = dataiku.core.schema_handling.get_schema_from_df(df)
    ds.set_schema({{"columns" : result_schema}})
    ds = dataiku.Dataset(ds_name)
    ds.write_with_schema(df)
""".format(json.dumps(result_data))
        self.recipe_string += ("\n \n \n" +dataset_output_string)

class SQLRecipeBuilder(RecipeBuilder):
    def __init__(self, project_key, klass, klass_config, task_parameters):
        RecipeBuilder.__init__(self, project_key, klass, klass_config, task_parameters)
        klass_instance = self.klass(**self.klass_config)
        klass_instance.set_task_params(**{_BASE_FLOW_GLOBAL_PARAMETERS_LABEL:self.task_parameters})
        self.engine_type = getattr(klass_instance, "engine", "Redshift")
        self.query = klass_instance.compose_query(klass_instance.base_query)
        self.connection_dataset_name = "{0}_query".format(self.klass.__name__)

    def _get_required_datasets(self):
        requires = RecipeBuilder._get_required_datasets(self)
        requires.append(self.connection_dataset_name)
        return requires

    def build(self):
        self.__build_sql_connection_dataset()
        RecipeBuilder.build(self)

    def __build_sql_connection_dataset(self):
        connection_str = "{0}DEV".format(self.engine_type)
        query_schema = self.api_client.sql_query(self.query + "  limit 10", connection_str).get_schema()

        params = {u'assumedTzForUnknownTz': u'UTC',
         u'connection': connection_str,
         u'distributionStyle': u'AUTO',
         u'mode': u'query',
         u'normalizeDoubles': True,
         u'notReadyIfEmpty': False,
         u'partitioningType': u'custom',
         u'query': self.query,
         u'readColsWithUnknownTzAsDates': False,
         u'readSQLDateColsAsDSSDates': True,
         u'sortKey': u'NONE',
         u'sortKeyColumns': [],
         u'tableCreationMode': u'auto',
         u'writeInsertBatchSize': 10000,
         u'writeJDBCBadDataBehavior': u'DISCARD_ROW',
         u'writeWithCopyBadDataBehavior': u'NOVERIFY_ERROR'}
        self.project.create_dataset(self.connection_dataset_name, self.engine_type, params)
        ds = dataikuapi.dss.dataset.DSSDataset(self.api_client, self.project.project_key, self.connection_dataset_name)
        ds.set_schema({"columns":query_schema})

    def _add_class_code(self):
        klass_source_code = inspect.getsource(self.klass)
        overriden_methods = \
"""

    def _pull_data(self, **kwargs):
        #result = kwargs["{0}"]
        result = required_data["{0}"]
        return result
""".format(self.connection_dataset_name)
        self.recipe_string += ("\n \n \n" +klass_source_code+overriden_methods)

class S3RecipeBuilder(RecipeBuilder):
    def __init__(self, project_key, klass, klass_config, task_parameters):
        RecipeBuilder.__init__(self, project_key, klass, klass_config, task_parameters)
        klass_instance = self.klass(**self.klass_config)
        klass_instance.set_task_params(**{_BASE_FLOW_GLOBAL_PARAMETERS_LABEL:self.task_parameters})
        self.bucket = klass_instance.bucket
        self.key = klass_instance.key
        self.connection_dataset_name = "{0}_s3conn".format(self.klass.__name__)

    def get_dataiku_schema_of_s3_file(self):
        s3_client = boto3.client("s3", aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                                 aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
        obj = s3_client.get_object(Bucket=self.bucket, Key=self.key)
        f_like_obj = io.BytesIO(obj['Body'].read())

        data = pd.read_csv(f_like_obj, nrows=100)
        schema = dataiku.core.dataset.schema_handling.get_schema_from_df(data)
        return schema

    def _get_required_datasets(self):
        requires = RecipeBuilder._get_required_datasets(self)
        requires.append(self.connection_dataset_name)
        return requires

    def build(self):
        self.__build_s3_connection_dataset()
        RecipeBuilder.build(self)

    def __build_s3_connection_dataset(self):
        schema = self.get_dataiku_schema_of_s3_file()
        format_type = "csv"
        file_type = "S3"
        params =  {'bucket': self.bucket,
                   'connection': u'ali_connection',
                   'filesSelectionRules': {'excludeRules': [],
                                           'explicitFiles': [],
                                           'includeRules': [],
                                           'mode': u'ALL'},
                   'notReadyIfEmpty': False,
                   'path': self.key}

        formatParams ={
              'separator': ','
               ,'style': 'excel'  # excel-style quoting
               ,'parseHeaderRow': True
               ,"normalizeDoubles": True
               ,"probableNumberOfRecords": len(schema)
               ,"separator": ","
        }

        self.project.create_dataset(self.connection_dataset_name, file_type, params, format_type, formatParams)
        ds = dataikuapi.dss.dataset.DSSDataset(self.api_client, self.project.project_key, self.connection_dataset_name)
        ds.set_schema({"columns":schema})

    def _add_class_code(self):
        klass_source_code = inspect.getsource(self.klass)
        overriden_methods = \
"""

    def _pull_data(self, **kwargs):
        #result = kwargs["{0}"]
        result = required_data["{0}"]
        return result
""".format(self.connection_dataset_name)
        self.recipe_string += ("\n \n \n" +klass_source_code+overriden_methods)

class DataIkuProjectBuilder(object):
    def __init__(self, ds_format_type, ds_filetype, ds_params, ds_format_params, task_params):
        self.ds_format_type = ds_format_type
        self.ds_filetype = ds_filetype
        self.ds_params = ds_params
        self.ds_format_params = ds_format_params
        self.api_client = dataiku.api_client()
        self.task_params = task_params
        self.project = None

    def build_project(self, project_key, pipeline_desc, project_name=None, owner = "DCM", delete_if_exists=False):
        if (project_key in self.api_client.list_project_keys()) & (delete_if_exists):
            deleter = DataIKUProjectDeleter(project_key)
            deleter.delete()
        self._create_project_and_store_handler(project_key, project_name, owner, pipeline_desc)
        self._build_all_datasets(pipeline_desc)
        self._build_all_recipes(pipeline_desc)

    def _create_project_and_store_handler(self, project_key, project_name, owner, pipeline_desc):
        if not project_name:
            project_name = project_key.lower()
        if project_key in self.api_client.list_project_keys():
            print("A project with project key = {0} already exists, if you wnat to recreate it, first delete the " +
                 "exisitng project using DataIKUProjectDeleter and then come back and create the project again".
                 format(project_key))
            if project_key == dataiku.default_project_key():
                print("The Current project is protected and cannot be deleted. you use the project key = {0}".
                     format(self.project_key))
                return

            project  =self. api_client.get_project(project_key)
            self.project = project
            return
        else:
            project = self.api_client.create_project(project_key=project_key,
                                             name = project_name,
                                             owner = owner,
                                             description = "First DCM Project")
            print("******************")
        self.project = project

    def _build_a_dataset(self, dataset_name):
        existing_datasets = [dataset["name"] for dataset in self.project.list_datasets()]
        if not (dataset_name in existing_datasets):
            params = json.loads(json.dumps(self.ds_params))
            params["path"] = params["path"].replace("${datasetName}", dataset_name)
            dataset = self.project.create_dataset(dataset_name,
                                                  type = self.ds_filetype,
                                                  formatType = self.ds_format_type,
                                                  params = params,
                                                  formatParams = self.ds_format_params)

            df = pd.DataFrame({"placeholder":[pd.np.NaN]})
            ds = dataikuapi.dss.dataset.DSSDataset(self.api_client, self.project.project_key, dataset_name)
            result_schema = dataiku.core.schema_handling.get_schema_from_df(df)
            ds.set_schema({"columns":result_schema})
            return dataset

        else:
            print("dataset {0} already exits".format(dataset_name))
        return None


    def _build_all_datasets(self, pipeline_desc):
        all_required_datasets = []
        for stage in pipeline_desc:
            for step in pipeline_desc.steps_iterator_for_stage(stage):
                step_required_data = set(CalibrationTaskRegistry.get_required_data_of(step))
                all_required_datasets.extend(step_required_data)
                step_produced_data = set(CalibrationTaskRegistry.get_produced_data_of(step))
                all_required_datasets.extend(step_produced_data)
        all_required_datasets = set(all_required_datasets)
        for ds_name in all_required_datasets:
            self._build_a_dataset(ds_name)

    def _build_a_recipe(self, step_klass, klass_config):
        regular_recipe_flag = (not issubclass(step_klass, (SQLReader, S3Reader)) or step_klass.__name__=="SQLMinuteToDailyEquityPrices"
                              or step_klass.__name__=="S3RussellComponentReader")
        print(regular_recipe_flag)
        recipe_builder_class = RecipeBuilder if regular_recipe_flag  else (SQLRecipeBuilder if issubclass(step_klass, SQLReader) else S3RecipeBuilder)
        recipe_builder = recipe_builder_class(self.project.project_key, step_klass, klass_config, self.task_params)
        recipe_builder.build()

    def _build_all_recipes(self, pipeline_desc):
        for stage in pipeline_desc:
            for step in pipeline_desc.steps_iterator_for_stage(stage):
                klass = CalibrationTaskRegistry.get_calibration_class(step)
                klass_config = pipeline_desc.get_configuration_of_step(klass.__name__)
                self._build_a_recipe(klass, klass_config)

    def _add_git_references_to_project(self, *args):
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Dataset Storage Options

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
format_type = "csv"
file_type = "Filesystem"
params =  {"path": u'/dataiku/${projectKey}/${datasetName}/data',
           "connection": "pipelines_data",
           "notReadyIfEmpty": False,
           'filesSelectionRules': {'excludeRules': [],
                                   'explicitFiles': [],
                                   'includeRules': [],
                                   'mode': u'ALL'}
          }
formatParams ={
      'separator': ','
       ,'style': 'excel'  # excel-style quoting
       ,'parseHeaderRow': True
   }

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Configuration

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
project_key = "QUANTAMENTAL_ML_NEW"
project_name = "Quantamental Machine learning (traditional)"
taskflow_project_path = ("/home/dataiku/dss/config/projects/{0}/lib/dcm/intuition/data_processing/"
                         "calibrations/quantamental_ml/quantamental_ml.conf").format(dataiku.default_project_key())

#project_key = "PIPELINE_FUNDAMENTALS_API"
#project_name = "Fundamental Trader Pipeline"
#taskflow_project_path = ("/home/dataiku/dss/config/projects/{0}/lib/dcm/intuition/data_processing/"
#                         "calibrations/fundamental_rebalancing_trader/fundamental_rebalancing_calibration.conf").format(dataiku.default_project_key())



task_params = BasicTaskParameters(pd.Timestamp("2018-12-03"), pd.Timestamp("2016-11-30"), pd.Timestamp("2018-11-30"),
                                 pd.Timestamp("1789-07-14"), TaskflowPipelineRunMode.Test)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Deleting Project

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
project_deleter = DataIKUProjectDeleter(project_key)
project_deleter.delete(True)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Importing Taskflow pipeline into DataIku

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
pipeline = TaskflowPipelineDescription(taskflow_project_path)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
project_builder = DataIkuProjectBuilder(format_type, file_type, params, formatParams, task_params)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
project_builder.build_project(project_key,
                              pipeline,
                              project_name=project_name,
                              delete_if_exists =False)