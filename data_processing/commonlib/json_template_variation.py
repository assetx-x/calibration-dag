import jsonxs
from sklearn.model_selection import ParameterGrid

import _pickle as cPickle

import itertools

class BasketPermutation(object):
    def __init__(self, equivalent_tickers):
        self.equivalent_tickers = equivalent_tickers

    def has_unique_elements(self, x):
        return len(x) == len(set(x))

    def get_all_permuations(self, basket):
        list_of_equivalnet_classes =[this_set for ticker in basket for this_set in self.equivalent_tickers
                                     if ticker in this_set]
        all_cartesian_product = list(itertools.product(*list_of_equivalnet_classes))
        cartesian_prod_without_repetition = [this_basket for this_basket in all_cartesian_product
                                             if self.has_unique_elements(this_basket)]
        return cartesian_prod_without_repetition

class JSONTemplateVariations(object):
    def __init__(self, json_template):
        self.json_template = json_template
        self.json_template_pickle = cPickle.dumps(json_template, -1)
        self.__default_val = lambda x:x

    def verify_paths_exist(self, paths):
        paths = [paths] if not hasattr(paths, "__iter__") or isinstance(paths, str) else paths
        values = {k:jsonxs.jsonxs(self.json_template, k, default=self.__default_val) for k in paths}
        not_valid_paths = [k for k in values if values[k] is self.__default_val]
        return {k:k not in not_valid_paths for k in paths}

    def get_template_values_for_paths(self, paths):
        paths = [paths] if not hasattr(paths, "__iter__") or isinstance(paths, str) else paths
        return {k:jsonxs.jsonxs(self.json_template, k) for k in paths}

    def replace_values_in_template(self, replacement_dict):
        new_object = cPickle.loads(self.json_template_pickle)
        [jsonxs.jsonxs(new_object, k, jsonxs.ACTION_SET, replacement_dict[k]) for k in replacement_dict]
        return new_object

    def get_variations_for_paths(self, param_grid):
        if not isinstance(param_grid, ParameterGrid):
            raise ValueError("JSONTemplateVariations.get_variations_for_paths - param_grid must be of type "
                             "ParameterGrid. Got {0}".format(type(param_grid)))
        grid_keys = reduce(lambda x,y:x.union(set(y)), param_grid, set())
        path_verification = self.verify_paths_exist(grid_keys)
        if not all(path_verification.values()):
            invalid_paths = [k for k in path_verification if not path_verification[k]]
            raise KeyError("JSONTemplateVariations.get_variations_for_paths - the parameter grid include paths that "
                           "are not compatible with template. The invalid paths are: {0}".format(invalid_paths))
        results = [{"replacements":k, "variation":self.replace_values_in_template(k)} for k in param_grid]
        return results


def basket_permutator_for_dict(dict_to_permute, equivalent_tickers):
    names_in_basket = dict_to_permute.keys()
    bp = BasketPermutation(equivalent_tickers)
    list_of_baskets = bp.get_all_permuations(names_in_basket)
    list_of_bakset_dict = []
    for basket in list_of_baskets:
        current_basket_dict = {}
        for k, name in enumerate(names_in_basket):
            current_basket_dict[basket[k]] = dict_to_permute[name]
        list_of_bakset_dict.append(current_basket_dict)
    return list_of_bakset_dict





if __name__=="__main__":
    equivalent_tickers = [set(["IJH","IWR","MDY"]), set(["IVV","IWB","IWV"]),set(["TLT","VCIT"])]

    from numpy import linspace
    from commonlib import jsonpickle_customization
    import jsonpickle
    import os
    sample_json_path = os.path.join("..", "..", "..", "strategies/cointegration/test/trading_actors",
                                    "ViewsInMarketTrader/ViewsInMarket_rebalance/configuration.json")


    with open(sample_json_path) as f:
        instructions = jsonpickle.loads("".join(f.readlines()), keys=True)

    instructions["instructions"]["views"]["vol_view"]["view_assets"][1] = {"IJH": 0.33, "IWV": 0.2, "TLT" : 0.47}
    current_basket = instructions["instructions"]["views"]["vol_view"]["view_assets"][1]


    variation_generator = JSONTemplateVariations(instructions)

    path1 = "instructions.views.vol_view.view_function_params.vol_thresh"
    path2 = "instructions.views.vol_view.view_assets[1]"
    path3 = "instructions.stop_limits_pct"
    paths = [path1, path2, path3]

    assert all(variation_generator.verify_paths_exist(paths).values())
    template_values = variation_generator.get_template_values_for_paths(paths)

    range1 = linspace(0.05, 0.15, 8)
    range2 = basket_permutator_for_dict(current_basket, equivalent_tickers)
    range3 = [(k,10.0+k) for k in linspace(-0.7, -0.2, 20)]
    ranges = [range1, range2, range3]
    param_variations = dict(zip(paths, ranges))

    p_grid = ParameterGrid(param_variations)
    template_variations = variation_generator.get_variations_for_paths(p_grid)
