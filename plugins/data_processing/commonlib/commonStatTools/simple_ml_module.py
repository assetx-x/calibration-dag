from sklearn.ensemble.partial_dependence import partial_dependence, plot_partial_dependence
from sklearn.model_selection import GroupKFold, GridSearchCV, cross_val_score
from sklearn.linear_model import LinearRegression, LassoCV, RidgeCV, ElasticNetCV
from sklearn.linear_model.coordinate_descent import _alpha_grid
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor, GradientBoostingRegressor
#from eli5.sklearn import PermutationImportance
from time import time
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
import pandas as pd
#import shap
import os

def train_ml_model(folds, X, y, algo, grid, n_jobs=11):
    print(algo.__class__)

    n_folds = folds.nunique()
    gkf = GroupKFold(n_splits=n_folds)
    splits = list(gkf.split(X, y, folds))

    tic = time()

    if grid is not None:
        cv = GridSearchCV(algo, grid, cv=splits, n_jobs=n_jobs, return_train_score=True)
        cv.fit(X, y)
        algo = cv.best_estimator_
        print(cv.best_params_)
        score = pd.DataFrame(cv.cv_results_).loc[cv.best_index_]
        score = score[score.index.str.startswith('split') & score.index.str.endswith('test_score')]

    else:
        if 'cv' in algo.get_params().keys():
            algo.set_params(cv=splits)
        algo.fit(X.values, y.values)
        score = pd.Series([algo.score(X.iloc[s[1]], y.iloc[s[1]]) for s in splits])

    toc = time()
    fit_time = (toc - tic) / 60.0
    lapsed = {'fit': fit_time}
    print(lapsed)

    return {
        'estimator': algo,
        'cv_score': score,
        'lapsed': lapsed
    }

if __name__=="__main__":
    
    print("done")