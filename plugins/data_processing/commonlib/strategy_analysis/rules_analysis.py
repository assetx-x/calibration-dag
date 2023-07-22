import commonlib.jsonlogic as jl
from analysis_dataframe_stats import get_summary_of_a_subset
import pandas as pd
from collections import namedtuple, OrderedDict
import seaborn as sns

RuleVariationInstructions = namedtuple("RuleVariationInstructions", ["rule_name", "subterm_path",
                                                                     "value_location_in_subterm", "value_override"])
RuleVariationInstructions.__str__ = lambda x:"{0}.{1}[{2}] ----> {3}".format(*x)

def get_subcut_and_res_for_query(data, query, return_col= "return_long"):
    subcut = data.query(query)
    result = get_summary_of_a_subset(subcut, return_col)
    return query, result, subcut

def evaluate_rule_aux(this_rule, full_df, return_col="return_long", return_as_dict=True):
    query = jl.jsonLogic(this_rule, operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)
    print(query)
    selection = full_df.query(query)
    result = get_summary_of_a_subset(selection, return_col)
    if return_as_dict:
        return result, selection
    else:
        print(query)
        pretty_print(result)
        return None, selection

def evaluate_rule(ruleset, rule_name, full_df, return_col="return_long", return_as_dict=True):
    this_rule = ruleset[rule_name]
    result, selection = evaluate_rule_aux(this_rule, full_df, return_col, return_as_dict)
    return result, selection

def compare_rules(full_df,ruleset, rule1, rule2, return_col="return_long", color_result_df=True):
    query1 = jl.jsonLogic(ruleset[rule1], operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)
    query2 = jl.jsonLogic(ruleset[rule2], operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)
    selections = OrderedDict()
    selections[rule1] = full_df.query(query1)
    selections[rule2] = full_df.query(query2)
    selections["INTERSECTION"] = selections[rule1].ix[selections[rule1].index.isin(selections[rule2].index),:]
    shared = selections["INTERSECTION"].index
    selections["EXCLUSIVE_{0}".format(rule1)] = selectections[rule1][~selections[rule1].index.isin(shared)]
    selections["EXCLUSIVE_{0}".format(rule2)] = selections[rule2][~selections[rule2].index.isin(shared)]
    data = [get_summary_of_a_subset(selections[k], return_col) for k in selections.keys()]
    result = pd.DataFrame(data, index=selections.keys())
    if color_result_df:
        result = result.style.background_gradient(cmap=sns.light_palette("green", as_cmap=True))\
            .highlight_null(null_color='lightyellow')
    return result

def compare_rules_on_shared_intervals(full_df,ruleset, rule1, rule2, return_col="return_long", color_result_df=True):
    query1 = jl.jsonLogic(ruleset[rule1], operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)
    query2 = jl.jsonLogic(ruleset[rule2], operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)
    selections = OrderedDict()
    selections[rule1] = full_df.query(query1)
    selections[rule2] = full_df.query(query2)
    shared_intervals = set(selections[rule1]["interval"].unique()).intersection(selections[rule2]["interval"].unique())

    selections[rule1] = selections[rule1].ix[selections[rule1]["interval"].isin(shared_intervals)]
    selections[rule2] = selections[rule2].ix[selections[rule2]["interval"].isin(shared_intervals)]

    data = [get_summary_of_a_subset(selections[k], return_col) for k in selections.keys()]
    result = pd.DataFrame(data, index=selections.keys())
    if color_result_df:
        result = result.style.background_gradient(cmap=sns.diverging_palette(10, 126, as_cmap=True))\
            .highlight_null(null_color='lightyellow')
    return result

def vary_rule_in_parameter(full_df,ruleset, rule_variation_instructions, overrides=None, return_col="return_long",
                           color_result_df=True, function_to_apply=None):
    function_to_apply = function_to_apply or get_summary_of_a_subset
    assert isinstance(rule_variation_instructions, RuleVariationInstructions)
    assert hasattr(rule_variation_instructions.value_override, "__iter__") \
           and not isinstance(rule_variation_instructions.value_override, str)
    overrides = overrides or []
    assert hasattr(overrides, "__iter__") and not isinstance(overrides, str)
    not_valid_overrides = filter(lambda x:not isinstance(x, RuleVariationInstructions), overrides)
    valid_overrides = filter(lambda x:isinstance(x, RuleVariationInstructions), overrides)
    not_valid_overrides = not_valid_overrides or filter(lambda x:hasattr(x.value_override, "__iter__"), valid_overrides)
    if not_valid_overrides:
        raise ValueError("When overrides is provided, it must be a list of isntances of RuleVariationInstructions with "
                         "fixed values on the field value_override")

    rule_name, subterm_path, value_location_in_subterm, value_range  = rule_variation_instructions
    ruleset = deepcopy(ruleset)
    for k in valid_overrides:
        jmespath.search('{0}.{1}'.format(k.rule_name, k.subterm_path), ruleset)[k.value_location_in_subterm] = k.value_override

    base_term_path = '{0}.{1}'.format(rule_name, subterm_path)
    base_term_to_vary = jmespath.search(base_term_path, ruleset)
    base_term_to_vary[value_location_in_subterm] = ":VALUE:"
    base_query = jl.jsonLogic(ruleset[rule_name], operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)
    expression_varied = jl.jsonLogic(jmespath.search(".".join(base_term_path.split(".")[0:-1]), ruleset),
                                     operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)
    print(" Overrides: [{2}\n            ] \nBase query: {3}".format(expression_varied, base_term_path, "\n"
                                                                     .join(map(lambda x:"\n               {0}"
                                                                               .format(x),valid_overrides)), base_query))

    queries = OrderedDict([("{3}".format(rule_name, subterm_path, value_location_in_subterm, k),
                           [base_term_to_vary.__setitem__(value_location_in_subterm, k),
                            jl.jsonLogic(ruleset[rule_name],
                                         operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)][1])
                           for k in value_range])
    results = [function_to_apply(full_df.query(queries[k]), return_col) for k in queries.keys()]
    results = pd.DataFrame(results, index=queries.keys())
    results.index.name = rule_name
    if color_result_df:
        results = results.style.background_gradient(cmap=sns.light_palette("green", as_cmap=True))\
            .highlight_null(null_color='lightyellow')\
        .set_caption("<font color='darkblue' size=4> Varying: <strong>{0}</strong> from {1} to {2} </font>"
                     .format(expression_varied, min(value_range), max(value_range)))
    return results

def collect_all_rules_result(full_df, ruleset,return_col="return_long", color_result_df=True):
    all_rules = ruleset.keys()
    final_result = pd.DataFrame([evaluate_rule(ruleset, k, full_df,return_col, return_as_dict=True)[0]
                                 for k in all_rules], index=all_rules)
    if color_result_df:
        final_result = final_result.style.background_gradient(cmap=sns.diverging_palette(10, 126, as_cmap=True))
    return final_result

def combine_rules(full_df, rule_list,all_rules):
    full_subset = []
    for rule in rule_list:
        _, this_subset = evaluate_rule(all_rules, rule, full_df)
        full_subset.append(this_subset)
    return pd.concat(full_subset, axis=0).drop_duplicates()