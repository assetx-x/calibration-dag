from core_classes import GCPReader, download_yahoo_data, DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import EconDataCreator,ShapProcessor,ModelLoader


current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path, construct_destination_path


class EconFactorInterpretation(DataReaderClass):
    '''

    Unraveling Shap Values of Econ models ensemble

    '''


    def __init__(
        self,
        BASE_DIR,
        bucket,
        leaf_path,
        x_cols,
        y_cols,
            cut_off_date
    ):
        self.BASE_DIR = BASE_DIR
        self.bucket = bucket
        self.leaf_path = leaf_path
        self.X_cols = x_cols
        self.y_cols = y_cols
        self.cut_off_date = cut_off_date

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass



    def do_step_action(self, **kwargs):
        df_monthly = kwargs["normalized_data_full_population_with_foldId"]
        #df_monthly['date'] = df_monthly['date'].apply(pd.Timestamp)


        model_loader = ModelLoader(self.BASE_DIR,
                                   self.leaf_path,
                                   self.bucket)
        econ_data_creator = EconDataCreator(self.BASE_DIR, model_loader)

        complete_econ_shap_dictionary, x_econ = econ_data_creator.create_econ_data(df_monthly,
                                                                                   self.X_cols,
                                                                                   self.y_cols,
                                                                                   quarter_cut=self.cut_off_date)

        return self._get_additional_step_results(complete_econ_shap_dictionary,x_econ)


    def _get_additional_step_results(self,complete_econ_shap_dictionary,x_econ):

        return {
            "econ_rf": complete_econ_shap_dictionary['rf'],
            'econ_lasso': complete_econ_shap_dictionary['lasso'],
            'econ_enet': complete_econ_shap_dictionary['enet'],
            'econ_ols': complete_econ_shap_dictionary['ols'],
            "econ_et": complete_econ_shap_dictionary['et'],
            "econ_gbm": complete_econ_shap_dictionary['gbm'],
            "x_econ":x_econ

        }
