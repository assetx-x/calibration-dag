import pytest
import numpy as np
import math

from commonlib.commonStatTools.mdd_q_func import MDD_Q_FUNC

np.random.seed(2016)

@pytest.fixture(scope="module")
def initialize_MDD_func():
    """
    1. Initializing class for MDD_q_func calculation
    2. Initializing tabulated data for Qn and Qp calculation.
    Source: http://alumnus.caltech.edu/~amir/drawdown-jrnl.pdf, pg. 23
    """
    MDD_ = MDD_Q_FUNC()
    X_n  = MDD_.X_n
    Q_n  = MDD_.Q_n
    X_p  = MDD_.X_p
    Q_p  = MDD_.Q_p

    return (MDD_, X_n, Q_n, X_p, Q_p)

@pytest.fixture(scope="module")
def get_x_n_ranges_for_test(initialize_MDD_func):
    """
    Preparing test data of Qn at Xn
    4 test cases: left extrapolation, right extrapolation, grid points, and interpolation
    :param init_tabulated_data
    :return: list of dicts for testing Qn at Xn
    """
    _, X_n, Q_n, X_p, Q_p = initialize_MDD_func
    X_n_left_extrap  = np.random.uniform(0,X_n[0],100)
    X_n_right_extrap = np.random.uniform(X_n[-1],100,100)
    X_n_grid_point   = X_n
    X_n_interpol     = [0.03125, 0.0925, 0.125, 0.75, 1.25, 3.25, 4.75]

    xn_test_dict =  [
                        {'x_n_left_extrap' : X_n_left_extrap},
                        {'x_n_grid_point'  : X_n_grid_point},
                        {'x_n_right_extrap': X_n_right_extrap},
                        {'x_n_interpol'    : X_n_interpol}
                    ]
    return xn_test_dict

@pytest.fixture(scope="module")
def get_x_p_ranges_for_test(initialize_MDD_func):
    """
    Preparing test data of Qp at Xp
    4 test cases: left extrapolation, right extrapolation, grid points, and interpolation
    :param initialize_MDD_func
    :return: list of dicts for testing Qp at Xp
    """
    _, X_n, Q_n, X_p, Q_p = initialize_MDD_func
    X_p_left_extrap  = np.random.uniform(0,X_p[0],100)
    X_p_right_extrap = np.random.uniform(X_p[-1],10000,100)
    X_p_grid_point   = X_p
    X_p_interpol    = [0.03125, 0.55, 1.0, 15.0, 100.0, 500.0, 4500.0]

    xp_test_dict =  [
                        {'x_p_left_extrap' : X_p_left_extrap},
                        {'x_p_grid_point'  : X_p_grid_point},
                        {'x_p_right_extrap': X_p_right_extrap},
                        {'x_p_interpol'    : X_p_interpol}
                    ]
    return xp_test_dict

@pytest.fixture(scope="module")
def get_emdd_data_for_test():
    """
    Preparing test data for EMDD testing. Source of the data:
    http://alumnus.caltech.edu/~amir/mdd-risk.pdf (page 3)
    http://www.cs.rpi.edu/~magdon/talks/mdd_NYU04.pdf (slide 32)
    :return: list of dicts for testing EMDD
    """

    # {'Fund' : [annualized rturn, annualized volatility, T_years, EMDD]}
    emdd_test_dict = \
        [
            {'SP500'   : [ 0.1004, 0.1548, 24.25, -0.4456 ]},
            {'FTSE100' : [ 0.0701, 0.1666, 19.83, -0.5554 ]},
            {'NASDAQ'  : [ 0.1120, 0.2438, 19.42, -0.7787 ]},
            {'DCM'     : [ 0.1565, 0.0578,  3.08, -0.0477 ]},
            {'NLT'     : [ 0.0335, 0.1603,  3.08, -0.3135 ]},
            {'OIC'     : [ 0.1719, 0.0452,  1.16, -0.02493]},
            {'TGF'     : [ 0.0848, 0.0983,  4.58, -0.1584 ]}
        ]

    return emdd_test_dict

@pytest.mark.parametrize("dict_with_xn_values", get_x_n_ranges_for_test(initialize_MDD_func()))
def test_qn_func(initialize_MDD_func, dict_with_xn_values):
    gamma = 0.6266570686577501 # (math.pi/8.)**.5
    MDD_, X_n, Q_n, X_p, Q_p = initialize_MDD_func

    test_qn_list = []
    expected_qn_list = []
    key = list(dict_with_xn_values.keys())[0]

    if key == 'x_n_left_extrap':
        for elem in dict_with_xn_values[key]:
            test_qn = MDD_.get_qn(elem)
            test_qn_list.append(test_qn)
            expected_qn = gamma * (2 * elem)**0.5 # Qn_asymptotic left side
            expected_qn_list.append(expected_qn)
    elif key == 'x_n_grid_point':
        expected_qn_list = Q_n
        for elem in dict_with_xn_values[key]:
            test_qn = MDD_.get_qn(elem)
            test_qn_list.append(test_qn)
    elif key == 'x_n_right_extrap':
        for elem in dict_with_xn_values[key]:
            test_qn = MDD_.get_qn(elem)
            test_qn_list.append(test_qn)
            expected_qn = elem + 0.5 # Qn_asymptotic right side
            expected_qn_list.append(expected_qn)
    else:
        # expected_qn_list computed using linear interpolation of MS EXCEL
        expected_qn_list = [0.1699925, 0.31122950000000005, 0.369361, 1.17202, 1.70936, 3.74294, 5.24083]
        for elem in dict_with_xn_values[key]:
            test_qn = MDD_.get_qn(elem)
            test_qn_list.append(test_qn)

    np.testing.assert_array_almost_equal(test_qn_list, expected_qn_list)

@pytest.mark.parametrize("dict_with_xp_values", get_x_p_ranges_for_test(initialize_MDD_func()))
def test_qp_func(initialize_MDD_func, dict_with_xp_values):
    gamma = 0.6266570686577501  # (math.pi/8.)**.5
    MDD_, X_n, Q_n, X_p, Q_p = initialize_MDD_func

    test_qp_list = []
    expected_qp_list = []
    key = list(dict_with_xp_values.keys())[0]

    if key == 'x_p_left_extrap':
        for elem in dict_with_xp_values[key]:
            test_qp = MDD_.get_qp(elem)
            test_qp_list.append(test_qp)
            expected_qp = gamma * (2 * elem) ** 0.5 # Qp_asymptotic left side
            expected_qp_list.append(expected_qp)
    elif key == 'x_p_grid_point':
        expected_qp_list = Q_p
        for elem in dict_with_xp_values[key]:
            test_qp = MDD_.get_qp(elem)
            test_qp_list.append(test_qp)
    elif key == 'x_p_right_extrap':
        for elem in dict_with_xp_values[key]:
            test_qp = MDD_.get_qp(elem)
            test_qp_list.append(test_qp)
            expected_qp = 0.25 * math.log(elem) + 0.49088 # Qp_asymptotic right side
            expected_qp_list.append(expected_qp)
    else:
        # expected_qn_list computed using linear interpolation of MS EXCEL
        expected_qp_list = [0.144502, 0.4757073, 0.588642, 1.184918, 1.647113, 2.046885, 2.595416]
        for elem in dict_with_xp_values[key]:
            test_qp = MDD_.get_qp(elem)
            test_qp_list.append(test_qp)

    np.testing.assert_array_almost_equal(test_qp_list, expected_qp_list)

@pytest.mark.parametrize("dict_with_emdd_test_data", get_emdd_data_for_test())
def test_emdd_func(initialize_MDD_func, dict_with_emdd_test_data):
    MDD_, _, _, _, _ = initialize_MDD_func
    key = list(dict_with_emdd_test_data.keys())[0]
    ret, vol, T, expected_emdd = dict_with_emdd_test_data[key] # input data for EMDD test + expected_emdd

    # Computing test EMDD
    sharpe    = ret / vol
    x         = T/2 * (sharpe**2)
    test_emdd = MDD_.get_EMDD(ret, vol, T, x)

    np.testing.assert_almost_equal(test_emdd, expected_emdd, decimal=2)
