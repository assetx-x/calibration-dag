from enum import Enum
import os
import sys

from numpy.random import normal
import pyhocon
from numpy.random import normal, seed
from numpy import sqrt, sin, pi, linspace, sign, roll, ones, asarray, expand_dims

# from tensorflow.keras.layers import Input, Dense, Reshape, Flatten, Dropout, LSTM, TimeDistributed, Lambda
# from tensorflow.keras.layers import concatenate, multiply, Dot, subtract, Multiply
# from tensorflow.keras.models import Sequential, Model
# from tensorflow.keras.optimizers import Adam
# from tensorflow.keras import losses
# from tensorflow.keras.backend import sum as k_sum, tile, stack, expand_dims as k_expand_dims, square, constant
# from tensorflow.keras.utils import plot_model
# from tensorflow.keras.callbacks import TensorBoard, ModelCheckpoint, ReduceLROnPlateau

import pandas as pd
import numpy as np
from numpy import inf, ceil

# from tqdm import tnrange, tqdm_notebook
from pathlib import Path

__config = None


def get_config():
    global __config
    if not __config:
        config_str = """
        sdf = {
           n_rnn_hidden_layers = 1
           rnn_hidden_layer = {
              units = 4
              unit_forget_bias = True
              dropout = 0.05
              recurrent_dropout = 0.05
              implementation = 1
              return_sequences=True
              use_bias = True
              #stateful = True
           }
           n_ffn_hidden_layers = 2
           ffn_hidden_layer = {
              units = 64
              activation = relu
           }
           ffn_droput_layer = {
              rate = 0.05
           }
           output_layer_units_multiplier = 1
        }
        conditional = {
           n_rnn_hidden_layers = 1
           rnn_hidden_layer = {
              units = 16
              unit_forget_bias = True
              dropout = 0.05
              recurrent_dropout = 0.05
              implementation = 1
              return_sequences=True
              use_bias = True
              #stateful = True
           }
           n_ffn_hidden_layers = 0
           ffn_hidden_layer = {
              units = 8
              activation = relu
           }
           ffn_droput_layer = {
              rate = 0.05
           }
           output_layer = {
           }
           output_layer_units_multiplier = 8
        }
        optimizer = {
           learning_rate = 0.001ret_2

        }
        data = {
            data_splits = [0.7, 0.1, 0.2]
            data_split_integer_override = [240, 0, 20]
            y_col = future_ret_21B_std
            # x_cols = [divyield, WILLR_14_MA_3, macd, log_dollar_volume, bm, retvol, netmargin, ADX_5_MA_3, ret_252B, ps,
            #           ULTOSC_7_14_28_MA_3, maxret, shareswa_growth, ret_2B, ADX_63_MA_3, asset_growth, PPO_12_26, macd_diff,
            #           ret_63B, ret_126B, pe, WILLR_63, debt2equity, chcsho, ADX_14, bm_ia_indgrp, gp2assets, std_turn,
            #           WILLR_63_MA_3, STOCHRSI_FASTD_14_5_3, fcf_yield, sharey, WILLR_14, macd_centerline, bm_ia_sector,
            #           dolvol, ADX_5, ebitda_to_ev, ret_10B, STOCHRSI_FASTK_14_5_3, rel_to_high, TRIX_30, ret_1B,
            #           log_avg_dollar_volume, SPY_beta, log_mktcap, ret_5B, ret_21B, volatility_63, volatility_126,
            #           momentum, macd_diff_indgrp, volatility_126_indgrp, ret_2B_indgrp, netmargin_indgrp, macd_indgrp,
            #           PPO_12_26_indgrp, overnight_return_indgrp, shareswa_growth_indgrp]
            # econ_cols = null
            x_cols = [volatility_126, log_mktcap, volatility_21, ret_126B, PPO_21_126_indgrp, WILLR_14, STOCHRSI_FASTK_63_15_10, overnight_return,
                      divyield_indgrp, ret_63B_indgrp, PPO_21_126, PPO_12_26_indgrp, macd_diff, ret_252B, ret_63B, PPO_3_14, momentum, amihud,
                      ret_2B, rel_to_high, SPY_correl, PPO_12_26, SPY_beta, bm_indgrp, netmargin_indgrp, volatility_126_indgrp, ret_10B,
                      log_dollar_volume, maxret, assetsc, ret_21B, ps1, ret_1B, ncfdiv_yield, ps, revenue_yield, debt2equity_indgrp,
                      current_to_total_assets, gp2assets, pe_indgrp, retearn_to_total_assets, asset_growth, bm, macd_diff_indgrp,
                      rnd_to_revenue, fcf_yield, ebitda_to_ev_indgrp, investments_yield, retearn_to_liabilitiesc, divyield, capex_yield,
                      std_turn, ADX_14_MA_3, avg_dollar_volume, grossmargin, ebitda_to_ev, STOCHRSI_FASTD_14_5_3, currentratio, netmargin,
                      ncfcommon_yield, payoutratio, inventory_to_revenue, pe1, revenue_growth, ebitdamargin, debt2equity]
            econ_cols = [CPFF, EWG_close, IPDCONGD, CUSR0000SA0L5, WPSID61, CUSR0000SA0L2, HWI, PPO_21_126_Industrials, CPIULFSL,
                         macd_diff_ConsumerStaples, PCEPI, divyield_Industrials, WPSFD49207, CPIAUCSL, CPITRNSL, PPO_21_126_Financials,
                         EWS_close, bm_Financials, WPSFD49502, WPSID62, EXSZUSx, UEMP15T26, IPMANSICS, PPO_21_126_Energy,
                         PPO_21_126_ConsumerDiscretionary, PPO_21_126_InformationTechnology, VIXCLS, CUSR0000SAC,
                         bm_ConsumerDiscretionary, divyield_ConsumerStaples, SPY_close, DNDGRG3M086SBEA, CP3Mx, EWJ_volume, T5YFFM,
                         GS10, USTRADE, UEMP5TO14, DEXUSUK, PPO_21_126_Utilities, ACOGNO, divyield_InformationTechnology, T10YFFM,
                         bm_Utilities, CUMFNS, CES0600000008, FEDFUNDS, GS1, GS5, T1YFFM, AAAFFM, EXJPUSx, DCOILWTICO, SPY_volume,
                         DTCTHFNM, RETAILx, RPI, CPIMEDSL, IPMAT, DPCERA3M086SBEA]
            batch_size = 239 #60
            time_steps = 1 #60
        }
        """
        __config = pyhocon.ConfigFactory.parse_string(config_str)
    return __config


def load_data(data_dir):
    print("loading data with shapes.....")
    df = pd.read_csv(os.path.join(data_dir, "company_data.csv"), index_col=0).fillna(0.0)
    econ_data = pd.read_csv(os.path.join(data_dir, "econ_data_final.csv"), index_col=0)
    active_matrix = pd.read_csv(os.path.join(data_dir, "active_matrix.csv"), index_col=0).fillna(0)
    return_data = pd.read_csv(os.path.join(data_dir, "future_returns.csv"), index_col=0).fillna(0.0)

    # Needs conversion for fixed format
    df["date"] = df["date"].apply(pd.Timestamp)
    econ_data["date"] = econ_data["date"].apply(pd.Timestamp)
    active_matrix.index = pd.DatetimeIndex(list(map(pd.Timestamp, list(active_matrix.index))))
    return_data.index = pd.DatetimeIndex(list(map(pd.Timestamp, list(return_data.index))))

    company_data = df.set_index(["date", "ticker"]).sort_index().to_panel().transpose(1, 2, 0).fillna(0.0)
    econ_data = econ_data.set_index(["date"]).sort_index()

    # Enforce common timeline
    common_timeline = pd.DatetimeIndex(econ_data.index).intersection(pd.DatetimeIndex(company_data.items)) \
        .intersection(return_data.index)
    # This is just to make data sets a multiple of 10
    # common_timeline = common_timeline[1:]
    econ_data = econ_data[econ_data.index.isin(common_timeline)]
    econ_data = econ_data.dropna(axis=1, how="any")
    econ_data = econ_data.fillna(0.0)
    good_tickers = company_data.major_axis.intersection(active_matrix.columns).intersection(return_data.columns)
    active_matrix = active_matrix[active_matrix.index.isin(common_timeline)]
    active_matrix = active_matrix[good_tickers]
    company_data = company_data[common_timeline, good_tickers, :]
    return_data = return_data[return_data.index.isin(common_timeline)]
    return_data = return_data[good_tickers]
    print("company_data: {0}".format(company_data.shape))
    print("econ_data: {0}".format(econ_data.shape))
    print("return_data: {0}".format(return_data.shape))
    print("active_matrix: {0}".format(active_matrix.shape))
    return company_data, econ_data, return_data, active_matrix

def build_timeseries_modified(econ_data, company_data, return_data, per_moment_time_weights, per_company_time_weights,
                              average_weights, time_steps):
    from tqdm import tnrange, tqdm_notebook
    dim_time = company_data.shape[0] - time_steps  # time
    n_assets = company_data.shape[1]  # comapny
    n_company_features = company_data.shape[2]  # characteristic
    n_macro_features = econ_data.shape[1]  # econ features
    econ = np.zeros((dim_time, time_steps, n_macro_features))
    comp = np.zeros((dim_time, time_steps, n_assets, n_company_features))
    rets = np.zeros((dim_time, time_steps, n_assets))
    mom_weights = np.zeros((dim_time, n_assets))
    comp_weights = np.zeros((dim_time, n_assets))
    avg_weights = np.zeros((dim_time, 1))
    neg_ones = -np.ones((dim_time, time_steps, 1))
    pos_ones = np.ones((dim_time, time_steps, 1))
    output = np.zeros((dim_time, time_steps))

    for i in tqdm_notebook(range(dim_time)):
        econ[i] = econ_data[i:time_steps + i]
        comp[i] = company_data[i:time_steps + i]
        rets[i] = return_data[i:time_steps + i]
        mom_weights[i] = per_moment_time_weights
        comp_weights[i] = per_company_time_weights
        avg_weights[i] = average_weights
    # print("length of time-series i/o", econ.shape, comp.shape, returns.shape, output.shape)
    return econ, comp, rets, mom_weights, comp_weights, avg_weights, neg_ones, pos_ones, output


class DataGeneratorMultiBatchFast(object):

    def _imports(self):
        from tensorflow.keras.layers import Input, Dense, Reshape, Flatten, Dropout, LSTM, TimeDistributed, Lambda
        from tensorflow.keras.layers import concatenate, multiply, Dot, subtract, Multiply
        from tensorflow.keras.models import Sequential, Model
        from tensorflow.keras.optimizers import Adam
        from tensorflow.keras import losses
        from tensorflow.keras.backend import sum as k_sum, tile, stack, expand_dims as k_expand_dims, square, constant
        from tensorflow.keras.utils import plot_model
        from tensorflow.keras.callbacks import TensorBoard, ModelCheckpoint, ReduceLROnPlateau
        print(f'[DataGeneratorMultiBatchFast] imports')

    def __init__(self, data, x_cols, econ_cols, y_col, batch_size, time_steps, data_splits=None,
                 data_split_integer_override=None):
        self._imports()
        data_splits = data_splits or [0.7, 0.1, 0.2]
        company_data, econ_data, return_data, active_matrix = data
        nobs = company_data.shape[0]  # - time_steps
        n_assets = company_data.shape[1]
        econ_cols = econ_cols or econ_data.columns
        if data_split_integer_override:
            self.n_train = data_split_integer_override[0]
            self.n_valid = data_split_integer_override[1]
            self.n = nobs
        else:
            self.n_train = int(nobs * data_splits[0])
            self.n_valid = int(nobs * data_splits[1])
            self.n = nobs
        self.batch_size = batch_size
        self.time_steps = time_steps
        self.n_assets = n_assets
        self.macro_factor = econ_data[econ_cols].fillna(method="ffill")
        self.characteristics = company_data[:, :, x_cols]
        self.target = company_data[:, :, y_col].T
        self.active = active_matrix
        self.returns = return_data
        self.average_weight = asarray([1.0 / self.n_assets])
        self.generation_mode = TrainingMode.sdf
        self.train_data_holder = None
        self.valid_data_holder = None
        self.test_data_holder = None
        self.build_train_data()
        if self.n_valid > 0:
            self.build_validation_data()
        # build train data later before testing
        # self.build_test_data()

    def build_train_data(self):
        initial_index = 0
        final_index = self.n_train
        current_active = self.active.iloc[initial_index:final_index]
        nobs = len(current_active)
        time_fragments = (1.0 * current_active.sum(axis=0)).values
        per_moment_time_weights = 1.0 / time_fragments
        per_company_time_weights = time_fragments / nobs
        per_moment_time_weights[per_moment_time_weights == inf] = 0.0
        per_moment_time_weights[per_company_time_weights == -inf] = 0.0
        per_company_time_weights[per_moment_time_weights == inf] = 0.0
        per_company_time_weights[per_company_time_weights == -inf] = 0.0

        # This may be redundant but is done for better readability for now
        macro_factor = self.macro_factor.iloc[initial_index:final_index, :].values
        characteristics = self.characteristics[self.characteristics.items[initial_index:final_index], :, :].values
        returns = self.returns.iloc[initial_index:final_index].values
        average_weights = self.average_weight * (-1.0 if self.generation_mode is TrainingMode.conditional else 1.0)

        econ, comp, rets, mom_weights, comp_weights, avg_weights, neg_ones, pos_ones, out = \
            build_timeseries_modified(macro_factor, characteristics, returns, per_moment_time_weights,
                                      per_company_time_weights,
                                      average_weights, self.time_steps)
        self.train_data_holder = (econ, comp, rets, mom_weights, comp_weights, avg_weights, neg_ones, pos_ones, out)

    def build_validation_data(self):
        initial_index = self.n_train
        final_index = self.n_train + self.n_valid
        current_active = self.active.iloc[initial_index:final_index]
        nobs = len(current_active)
        time_fragments = (1.0 * current_active.sum(axis=0)).values
        per_moment_time_weights = np.nan_to_num(1.0 / time_fragments)
        per_company_time_weights = np.nan_to_num(time_fragments / nobs)
        per_moment_time_weights[per_moment_time_weights == inf] = 0.0
        per_moment_time_weights[per_company_time_weights == -inf] = 0.0
        per_company_time_weights[per_moment_time_weights == inf] = 0.0
        per_company_time_weights[per_company_time_weights == -inf] = 0.0

        # This may be redundant but is done for better readability for now
        macro_factor = self.macro_factor.iloc[initial_index:final_index, :].values
        characteristics = self.characteristics[self.characteristics.items[initial_index:final_index], :, :].values
        returns = self.returns.iloc[initial_index:final_index].values

        econ, comp, rets, mom_weights, comp_weights, avg_weights, neg_ones, pos_ones, out = \
            build_timeseries_modified(macro_factor, characteristics, returns, per_moment_time_weights,
                                      per_company_time_weights,
                                      average_weights, self.time_steps)
        self.valid_data_holder = (econ, comp, rets, mom_weights, comp_weights, avg_weights, neg_ones, pos_ones, out)

    def build_test_data(self):
        pass

    def set_training_mode(self, training_mode):
        self.generation_mode = training_mode

    def generate_train_data(self):
        econ, comp, rets, mom_weights, comp_weights, avg_weights, neg_ones, pos_ones, out = self.train_data_holder

        start_index = 0
        while True:
            # Termination is controlled by steps per epoch
            end_index = start_index + self.batch_size
            inputs = [econ[start_index:end_index], comp[start_index:end_index], rets[start_index:end_index],
                      mom_weights[start_index:end_index], comp_weights[start_index:end_index],
                      avg_weights[start_index:end_index],
                      neg_ones[start_index:end_index], pos_ones[start_index:end_index]]
            outputs = out[start_index:end_index]
            start_index += self.time_steps
            yield (inputs, outputs)

    def generate_validation_data(self):
        econ, comp, rets, mom_weights, comp_weights, avg_weights, neg_ones, pos_ones, out = self.valid_data_holder

        start_index = 0
        while True:
            # Termination is controlled by steps per epoch
            end_index = start_index + self.batch_size
            inputs = [econ[start_index:end_index], comp[start_index:end_index], rets[start_index:end_index],
                      mom_weights[start_index:end_index], comp_weights[start_index:end_index],
                      avg_weights[start_index:end_index],
                      neg_ones[start_index:end_index], pos_ones[start_index:end_index]]
            outputs = out[start_index:end_index]
            start_index += self.time_steps
            yield (inputs, outputs)

    def generate_test_data(self):
        initial_index = self.n_train + self.n_valid
        final_index = self.n
        current_active = self.active.iloc[initial_index:final_index]
        nobs = len(current_active)
        time_fragments = (1.0 * current_active.sum(axis=0)).values
        per_moment_time_weights = np.nan_to_num(1.0 / time_fragments)
        per_company_time_weights = np.nan_to_num(time_fragments / nobs)
        per_moment_time_weights[per_moment_time_weights == inf] = 0.0
        per_moment_time_weights[per_company_time_weights == -inf] = 0.0
        per_company_time_weights[per_moment_time_weights == inf] = 0.0
        per_company_time_weights[per_company_time_weights == -inf] = 0.0

        macro_factor = self.macro_factor.iloc[initial_index:final_index, :].values
        characteristics = self.characteristics[self.characteristics.items[initial_index:final_index], :, :].values
        returns = self.returns.iloc[initial_index:final_index].values

        econ, comp, rets, mom_weights, comp_weights, avg_weights, neg_ones, pos_ones, out = \
            build_timeseries_modified(macro_factor, characteristics, returns, per_moment_time_weights,
                                      per_company_time_weights,
                                      average_weights, self.time_steps)

        # Termination is controlled by steps per epoch
        inputs = [econ, comp, rets, mom_weights, comp_weights, avg_weights,
                  neg_ones, pos_ones]
        outputs = out[start_index:end_index]
        return (inputs, outputs)

    def get_full_data(self):
        initial_index = 0
        final_index = self.n
        current_active = self.active.iloc[initial_index:final_index]
        nobs = len(current_active)
        time_fragments = (1.0 * current_active.sum(axis=0)).values
        per_moment_time_weights = 1.0 / time_fragments
        per_company_time_weights = time_fragments / nobs
        per_moment_time_weights[per_moment_time_weights == inf] = 0.0
        per_moment_time_weights[per_company_time_weights == -inf] = 0.0
        per_company_time_weights[per_moment_time_weights == inf] = 0.0
        per_company_time_weights[per_company_time_weights == -inf] = 0.0

        # This may be redundant but is done for better readability for now
        macro_factor = self.macro_factor.iloc[initial_index:final_index, :].values
        characteristics = self.characteristics[self.characteristics.items[initial_index:final_index], :, :].values
        returns = self.returns.iloc[initial_index:final_index].values
        average_weights = self.average_weight * (-1.0 if self.generation_mode is TrainingMode.conditional else 1.0)

        econ, comp, rets, mom_weights, comp_weights, avg_weights, neg_ones, pos_ones, out = \
            build_timeseries_modified(macro_factor, characteristics, returns, per_moment_time_weights,
                                      per_company_time_weights,
                                      average_weights, self.time_steps)

        # Hack to save time for predictions
        self.full_return_sequences = rets
        return [econ, comp]


class AssetPricingGAN(object):
    def __init__(self, n_macro_features=1, n_company_fetures=2, n_assets=1, time_steps=250, batch_size=10,
                 optimizer="adam",
                 training_loss="mean_squared_error"):
        self.n_macro_features = n_macro_features
        self.n_company_fetures = n_company_fetures
        self.n_assets = n_assets
        self.time_steps = time_steps
        self.optimizer = optimizer
        self.training_loss = training_loss
        self.batch_size = batch_size

        self.create_input_layers()
        self.sdf_network = self.__build_network("sdf")
        self.conditional_network = self.__build_network("conditional")
        final_loss = self.create_loss()

        self.gan_model = Model([self.macro_input_layer, self.company_input_layer, self.return_sequences,
                                self.moment_weight_input_layer, self.company_weight_input_layer,
                                self.overall_loss_weight_input_layer,
                                self.discount_factor_multiplier, self.discount_factor_substraction],
                               final_loss)
        self.compile_model("Initial network architecture")
        # plot_model(self.gan_model, os.path.join(data_dir, "full_asset_pricing_model_architecture.png"), show_shapes=True)
        print("Full model compiled")

    def prepare_training_callbacks(self, model_saving_period):
        tensorboard_path = "/nn_results/gan_model/model/run_3_mb/tensorboard/test"
        model_path = "/nn_results/gan_model/model/run_3_mb/model/test/gan-model-{epoch:03d}_{loss:.2f}.hdf5"
        create_directory_if_does_not_exists(tensorboard_path)
        print(os.path.dirname(model_path))
        create_directory_if_does_not_exists(os.path.dirname(model_path))
        delete_directory_content(tensorboard_path)
        delete_directory_content(os.path.dirname(model_path))
        # keras_callbacks= [TensorBoard(log_dir = tensorboard_path,
        #                              histogram_freq=2,
        #                              write_graph=True,
        #                              write_images=False,
        #                              update_freq='epoch',
        #                              profile_batch=0),
        #                  ModelCheckpoint(filepath=model_path,
        #                                  verbose=1, save_weights_only=True, period=model_saving_period),
        #                  ReduceLROnPlateau(monitor='loss', factor=0.9, patience=3, verbose=1, mode='auto',
        #                                    min_delta=0.001, cooldown=3, min_lr=0.000001)]
        keras_callbacks = [ReduceLROnPlateau(monitor='loss', factor=0.9, patience=3, verbose=1, mode='auto',
                                             min_delta=0.000001, cooldown=3, min_lr=0.00000001)]
        return keras_callbacks

    def compile_model(self, msg=""):
        # self.gan_model.to_yaml()
        self.gan_model.compile(optimizer=self.optimizer, loss=self.training_loss)
        print(self.gan_model.summary())
        print(msg)

    def create_loss(self):
        f_factor = Dot(2, name="f_factor")([self.return_sequences, self.sdf_network.output])
        # discount_factor = Multiply(name="negative_f_factor")([self.discount_factor_multiplier, f_factor])
        negative_f_factor = multiply([self.discount_factor_multiplier, f_factor], name="negative_f_factor")
        discount_factor = subtract([self.discount_factor_substraction, negative_f_factor], name="discount_factor")
        # tiled_returns = tile(k_expand_dims(self.return_sequences), (1,1,1,self.conditional_network.output.shape[3]))
        tiled_returns = Lambda(lambda x: stack([x for i in range(8)], 3), name="tiled_returns")(self.return_sequences)
        tiled_discount_factor = Lambda(
            lambda x: tile(k_expand_dims(x), (1, 1,) + tuple(self.conditional_network.output.shape[2:])),
            name="tiled_discount_factor")(discount_factor)

        moment_factors = multiply([tiled_discount_factor, tiled_returns, self.conditional_network.output],
                                  name="moment_factors_terms")
        time_averaged_loss = Lambda(lambda x: k_sum(x, axis=1), name="time_averaged_losses_per_company_moment")(
            moment_factors)
        tiled_moment_weights = Lambda(lambda x: tile(k_expand_dims(x), (1, 1, time_averaged_loss.shape[-1])),
                                      name="tiled_moment_weights")(self.moment_weight_input_layer)
        weighted_moment_factors = multiply([tiled_moment_weights, time_averaged_loss], name="weighted_moments")
        per_company_loss = Lambda(lambda x: k_sum(square(x), axis=2), name="loss_per_company")(weighted_moment_factors)
        weighted_per_company_loss = multiply([self.company_weight_input_layer, per_company_loss],
                                             name="weighted_loss_per_company")
        total_loss = Lambda(lambda x: k_sum(x, 1), name="overall_loss")(weighted_per_company_loss)
        final_loss = multiply([self.overall_loss_weight_input_layer, total_loss], name="final_loss")
        return final_loss

    def set_training_mode(self, training_mode):
        layers_to_freeze_prefix = "conditional" if training_mode is TrainingMode.sdf else "sdf"
        for k in self.gan_model.layers:
            k.trainable = True
        layers_to_freeze = [k for k in self.gan_model.layers if k.name.startswith(layers_to_freeze_prefix)]
        for k in layers_to_freeze:
            k.trainable = False
        self.compile_model("Architecture of model set to train {0}".format(training_mode))

    def create_input_layers(self):
        self.macro_input_layer = Input(shape=(self.time_steps, self.n_macro_features), name="macro_data")
        self.company_input_layer = Input(shape=(self.time_steps, self.n_assets, self.n_company_fetures),
                                         name="company_data")
        self.return_sequences = Input(shape=(self.time_steps, self.n_assets), name="company_returns")

        self.moment_weight_input_layer = Input(shape=(self.n_assets,), name="weight_per_moment")
        self.company_weight_input_layer = Input(shape=(self.n_assets,), name="weight_per_company")
        self.overall_loss_weight_input_layer = Input(shape=(1,), name="loss_weight")

        self.discount_factor_multiplier = Input(shape=(self.time_steps, 1), name="discount_factor_multiplier")
        self.discount_factor_substraction = Input(shape=(self.time_steps, 1), name="discount_factor_substraction")

        self.flattened_company_inputs = Reshape((self.time_steps or -1, self.n_assets * self.n_company_fetures,),
                                                name="flat_company_data")(self.company_input_layer)

    def __build_network(self, cfg_section_name):
        cfg_section = get_config()[cfg_section_name]
        lstm_layer = self.macro_input_layer
        for k in range(cfg_section["n_rnn_hidden_layers"]):
            lstm_layer = LSTM(**cfg_section["rnn_hidden_layer"], name="{0}-rnn-{1}".format(cfg_section_name, k))(
                lstm_layer)
        ffn_input = concatenate([lstm_layer, self.flattened_company_inputs],
                                name="{0}-ffn-input".format(cfg_section_name))
        ffn = ffn_input
        for k in range(cfg_section["n_ffn_hidden_layers"]):
            ffn = TimeDistributed(Dense(**cfg_section["ffn_hidden_layer"]),
                                  name="{0}-fnn-{1}".format(cfg_section_name, k + 1))(ffn)
            if cfg_section.get("ffn_droput_layer", None) and cfg_section["ffn_droput_layer"]["rate"]:
                ffn = TimeDistributed(Dropout(**cfg_section["ffn_droput_layer"]),
                                      name="{0}-dropout-{1}".format(cfg_section_name, k + 1))(ffn)
        output_multiplier = cfg_section["output_layer_units_multiplier"]
        ffn_output_raw = TimeDistributed(Dense(self.n_assets * output_multiplier),
                                         name="{0}-output-raw".format(cfg_section_name))(ffn)
        ffn_output = Reshape((self.time_steps, self.n_assets, output_multiplier),
                             name="{0}-output".format(cfg_section_name))(ffn_output_raw)

        network_model = Model([self.macro_input_layer, self.company_input_layer], ffn_output)
        # plot_model(network_model, os.path.join(data_dir, "full_asset_pricing_model_architecture_{0}.png".format(cfg_section_name)), show_shapes=True)
        return network_model

    def train_network(self, data_generator, epochs=1, epochs_discriminator=None, gan_iterations=1, verbose=1,
                      run_validation=False, shuffle=False):
        epoch_counter = 0
        cbs = self.prepare_training_callbacks(epochs)
        # Too much space
        # cbs = None
        steps_per_epoch = 1
        for gan_iter in range(gan_iterations):
            for training_mode in [TrainingMode.sdf, TrainingMode.conditional]:
                print("Setting training mode of gan_iteration #{0} to {1}".format(gan_iter, training_mode))
                if training_mode == TrainingMode.sdf:
                    current_epochs = epochs
                else:
                    current_epochs = epochs_discriminator or epochs
                self.set_training_mode(training_mode)
                data_generator.set_training_mode(training_mode)
                if run_validation:
                    self.history = self.gan_model.fit_generator(data_generator.generate_train_data(), steps_per_epoch,
                                                                epoch_counter + current_epochs, verbose,
                                                                validation_data=data_generator.generate_validation_data(),
                                                                validation_steps=steps_per_epoch, validation_freq=1,
                                                                callbacks=cbs, initial_epoch=epoch_counter,
                                                                shuffle=shuffle)
                else:
                    self.history = self.gan_model.fit_generator(data_generator.generate_train_data(), steps_per_epoch,
                                                                epoch_counter + current_epochs, verbose,
                                                                callbacks=cbs, initial_epoch=epoch_counter,
                                                                shuffle=shuffle)
                epoch_counter += epochs

    def train_network_stateful(self, data_generator, epochs=1, epochs_discriminator=None, gan_iterations=1, verbose=1,
                               run_validation=True,
                               shuffle=False):
        epoch_counter = 0
        cbs = self.prepare_training_callbacks(epochs)
        nobs = data_generator.n_train - self.time_steps
        nobs_val = data_generator.n_valid - self.time_steps
        steps_per_epoch = int(ceil(nobs / self.batch_size))
        validation_steps_per_epoch = int(ceil(nobs_val / self.batch_size))
        for gan_iter in range(gan_iterations):
            for training_mode in [TrainingMode.sdf, TrainingMode.conditional]:
                print("Setting training mode of gan_iteration #{0} to {1}".format(gan_iter, training_mode))
                if training_mode == TrainingMode.sdf:
                    current_epochs = epochs
                else:
                    current_epochs = epochs_discriminator or epochs
                self.set_training_mode(training_mode)
                data_generator.set_training_mode(training_mode)
                for i in range(current_epochs):
                    print("epoch {0}".format(i))
                    if run_validation:
                        self.history = self.gan_model.fit_generator(data_generator.generate_train_data(),
                                                                    steps_per_epoch, epoch_counter + 1, verbose,
                                                                    validation_data=data_generator.generate_validation_data(),
                                                                    validation_steps=validation_steps_per_epoch,
                                                                    validation_freq=1,
                                                                    callbacks=cbs, initial_epoch=epoch_counter,
                                                                    shuffle=shuffle)
                    else:
                        self.history = self.gan_model.fit_generator(data_generator.generate_train_data(),
                                                                    steps_per_epoch, epoch_counter + 1, verbose,
                                                                    callbacks=cbs, initial_epoch=epoch_counter,
                                                                    shuffle=shuffle)
                    epoch_counter += 1
                    self.gan_model.reset_states()


class SDFExtraction(object):  # This is a complete version with data and gan network
    def __init__(self, data_dir, insample_cut_date):
        self.data_dir = data_dir
        self.insample_cut_date = pd.Timestamp(insample_cut_date)
        self.saved_weight_path = os.path.join(data_dir, "saved_weights.h5")
        self.data_config = get_config()["data"]
        self.all_factor_path = os.path.join(data_dir, "all_factors.h5")

        company_data, econ_data, return_data, active_matrix = self._load_data()

        n_assets = company_data.shape[1]
        data_splits = self.data_config["data_splits"]
        x_cols = self.data_config["x_cols"]
        y_col = self.data_config["y_col"]
        econ_cols = self.data_config["econ_cols"]
        time_steps = self.data_config["time_steps"]
        n_macro_features = len(econ_cols)
        n_company_features = len(x_cols)

        n_obs = len(active_matrix) - time_steps
        n_train = sum(active_matrix.index <= self.insample_cut_date) - time_steps
        n_test = n_obs - n_train
        data_split_integer_override = [n_train, 0, n_test]
        batch_size = n_train - time_steps

        self.data_generator = DataGeneratorMultiBatchFast((company_data, econ_data, return_data, active_matrix),
                                                          x_cols, econ_cols, y_col, batch_size, time_steps,
                                                          data_splits, data_split_integer_override)
        gan_network = AssetPricingGAN(n_macro_features, n_company_features, n_assets, time_steps=time_steps,
                                      batch_size=batch_size,
                                      training_loss=linear_pred_loss,
                                      optimizer=Adam(lr=0.00001, decay=0.0, epsilon=1e-8))

        self.n_macro_features = gan_network.n_macro_features
        self.n_company_fetures = gan_network.n_company_fetures
        self.n_assets = gan_network.n_assets
        self.time_steps = gan_network.time_steps
        self.batch_size = gan_network.batch_size
        self.gan_model = gan_network.gan_model
        if saved_weight_path:
            self.gan_model.load_weights(saved_weight_path)  # load weights
        self.create_input_layers()
        self.sdf_network = self.__build_network()
        self.weighted_layers = ["sdf-rnn-0", "sdf-fnn-1", "sdf-fnn-2", "sdf-output-raw"]
        print(self.sdf_network.summary())
        self.__populate_sdf_network_weights()
        self.intermediate_model = Model(inputs=self.sdf_network.input,
                                        outputs=self.sdf_network.get_layer("sdf-rnn-0").output)
        self.sdf_output = None
        self.return_sequences = None
        self.f_factor = None
        self.hidden_states = None
        self.all_factors = None

    def _load_data(self):
        return load_data(self.data_dir)

    def __populate_sdf_network_weights(self):
        for layer_name in self.weighted_layers:
            print("Populating weights for layer {0}".format(layer_name))
            weights = self.gan_model.get_layer(layer_name).get_weights()
            self.sdf_network.get_layer(layer_name).set_weights(weights)
        print("All Saved Weights Populated")

    def predict_sdf(self):
        nobs = self.data_generator.n  # - self.time_steps
        print(nobs)
        steps = int(ceil(nobs / self.batch_size))
        print(steps)
        self.sdf_output = self.sdf_network.predict(self.data_generator.get_full_data(), steps)
        self.return_sequences = self.data_generator.full_return_sequences
        self.norm_constant = self.sdf_output[:self.data_generator.n_train, 0, :].sum(axis=1).mean()

    def create_input_layers(self):
        self.macro_input_layer = Input(shape=(self.time_steps, self.n_macro_features), name="macro_data")
        self.company_input_layer = Input(shape=(self.time_steps, self.n_assets, self.n_company_fetures),
                                         name="company_data")
        self.return_sequences = Input(shape=(self.time_steps, self.n_assets), name="company_returns")
        self.flattened_company_inputs = Reshape((self.time_steps or -1, self.n_assets * self.n_company_fetures,),
                                                name="flat_company_data")(self.company_input_layer)

    def __build_network(self):
        cfg_section_name = "sdf"
        cfg_section = get_config()[cfg_section_name]
        lstm_layer = self.macro_input_layer
        for k in range(cfg_section["n_rnn_hidden_layers"]):
            lstm_layer = LSTM(**cfg_section["rnn_hidden_layer"], name="{0}-rnn-{1}".format(cfg_section_name, k))(
                lstm_layer)
        ffn_input = concatenate([lstm_layer, self.flattened_company_inputs],
                                name="{0}-ffn-input".format(cfg_section_name))
        ffn = ffn_input
        for k in range(cfg_section["n_ffn_hidden_layers"]):
            ffn = TimeDistributed(Dense(**cfg_section["ffn_hidden_layer"]),
                                  name="{0}-fnn-{1}".format(cfg_section_name, k + 1))(ffn)
            if cfg_section.get("ffn_droput_layer", None) and cfg_section["ffn_droput_layer"]["rate"]:
                ffn = TimeDistributed(Dropout(**cfg_section["ffn_droput_layer"]),
                                      name="{0}-dropout-{1}".format(cfg_section_name, k + 1))(ffn)
        output_multiplier = cfg_section["output_layer_units_multiplier"]
        ffn_output_raw = TimeDistributed(Dense(self.n_assets * output_multiplier),
                                         name="{0}-output-raw".format(cfg_section_name))(ffn)
        ffn_output = Reshape((self.time_steps, self.n_assets, output_multiplier),
                             name="{0}-output".format(cfg_section_name))(ffn_output_raw)

        network_model = Model([self.macro_input_layer, self.company_input_layer], ffn_output)
        # plot_model(network_model, os.path.join(data_dir, "full_asset_pricing_model_architecture_{0}.png".format(cfg_section_name)), show_shapes=True)
        return network_model

    def extract_hidden_states(self):
        intermediate_output = self.intermediate_model.predict(self.data_generator.get_full_data())
        states = intermediate_output[:, 0, :]
        econ_data = self.data_generator.macro_factor
        hidden_states = pd.DataFrame(states, index=econ_data.index[1:])
        hidden_states.columns = list(map(lambda x: "hidden_state_{0}".format(x), hidden_states.columns))
        self.hidden_states = hidden_states

    def construct_f_factor(self):
        econ_data = self.data_generator.macro_factor
        nobs = self.data_generator.n - self.data_generator.time_steps
        self.predict_sdf()
        raw = np.dot(self.return_sequences, self.sdf_output)
        self.f_factor = pd.Series(raw[:, 0, nobs - 1, 0, 0], index=econ_data.index[1:]) / self.norm_constant

    def generate_all_factors(self):
        self.extract_hidden_states()
        self.construct_f_factor()
        all_factors = pd.concat([self.f_factor.shift(), self.f_factor, self.hidden_states], axis=1). \
            rename(columns={0: "f_t", 1: "f_t+1"})
        mean_vals = all_factors[:self.insample_cut_date].mean()
        all_factors = all_factors.fillna(mean_vals)
        self.all_factors = all_factors

    def save_factors(self):
        print(self.all_factor_path)
        self.all_factors.to_hdf(self.all_factor_path, key="df", format="table")

def extract_factors(data_dir, insample_cut_date):
    sdf_network = SDFExtraction(data_dir, insample_cut_date)
    sdf_network.generate_all_factors()
    sdf_network.save_factors()