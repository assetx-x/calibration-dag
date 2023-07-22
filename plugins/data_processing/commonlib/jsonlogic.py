# This is a Python implementation of the following jsonLogic JS library:
# https://github.com/jwadhams/json-logic-js

import sys
from enum import Enum
from pyhocon import ConfigFactory, ConfigTree
from glob import glob
from os import path
from itertools import product

data = None

class JsonLogicOperationMode(Enum):
    Evaluation = 1
    ExpressionBuilder = 2

__operations_eval = {
    "=="  : (lambda a, b: a == b),
    "===" : (lambda a, b: a is b),
    "!="  : (lambda a, b: a != b),
    "!==" : (lambda a, b: a is not b),
    ">"   : (lambda a, b: a > b),
    ">="  : (lambda a, b: a >= b),
    "<"   : (lambda a, b, c=None:
             a < b if (c is None) else (a < b) and (b < c)
             ),
    "<="  : (lambda a, b, c=None:
             a <= b if (c is None) else (a <= b) and (b <= c)
             ),
    "!"   : (lambda a: not a),
    "%"   : (lambda a, b: a % b),
    "and" : (lambda *args:
             reduce(lambda total, arg: total and arg, args, True)
             ),
    "or"  : (lambda *args:
             reduce(lambda total, arg: total or arg, args, False)
             ),
    "?:"  : (lambda a, b, c: b if a else c),
    "log" : (lambda a: a if sys.stdout.write(str(a)) else a),
    "in"  : (lambda a, b:
             a in b if "__contains__" in dir(b) else False
             ),
    "not in"  : (lambda a, b:
             a not in b if "__contains__" in dir(b) else False
             ),
    "var" : (lambda a, not_found=None:
             reduce(lambda data, key: (data.get(key, not_found)
                                       if isinstance(data, dict)
                                  else data[int(key)]
                                  if (type(data) in [list, tuple] and
                                      str(key).lstrip("-").isdigit())
                                  else not_found),
                                 str(a).split("."),
                            data)
             ),
    "cat" : (lambda *args:
             "".join(args)
             ),
    "+" : (lambda *args:
           reduce(lambda total, arg: total + float(arg), args, 0.0)
           ),
    "*" : (lambda *args:
           reduce(lambda total, arg: total * float(arg), args, 1.0)
           ),
    "-" : (lambda a, b=None: -a if b is None else a - b),
    "/" : (lambda a, b=None: a if b is None else float(a) / float(b)),
    "min" : (lambda *args: min(args)),
    "max" : (lambda *args: max(args)),
    "count": (lambda *args: sum(1 if a else 0 for a in args)),
}

__operations_expressions = {
    "=="  : (lambda a, b: "({0} == {1})".format(a,b)),
    "!="  : (lambda a, b: "({0} != {1})".format(a,b)),
    ">"   : (lambda a, b: "({0} > {1})".format(a,b)),
    ">="  : (lambda a, b: "({0} >= {1})".format(a,b)),
    "<"   : (lambda a, b, c=None:
             "({0} < {1})".format(a,b) if (c is None) else "({0} < {1} and {1} < {2})".format(a,b,c)
             ),
    "<="  : (lambda a, b, c=None:
             "({0} <= {1})".format(a,b) if (c is None) else "({0} <= {1} and {1} <= {2})".format(a,b,c)
             ),
    "!"   : (lambda a: "(not {0})".format(a)),
    "%"   : (lambda a, b: "{0} % {1}".format(a, b)),
    "and" : (lambda *args: "({0})".format(" and ".join(list(args)))),
    "or"  : (lambda *args: "({0})".format(" or ".join(list(args)))),
    "log" : (lambda a: a if sys.stdout.write(str(a)) else a),
    "in"  : (lambda a, b: "({0} in {1})".format(a,b) if "__contains__" in dir(b) else ""),
    "not in"  : (lambda a, b: "({0} not in {1})".format(a,b) if "__contains__" in dir(b) else ""),
    "var" : (lambda a, not_found=None:
             reduce(lambda data, key: (data.get(key, not_found)
                                       if isinstance(data, dict)
                                  else data[int(key)]
                                  if (type(data) in [list, tuple] and
                                      str(key).lstrip("-").isdigit())
                                  else not_found),
                                 str(a).split("."),
                            data)
             ),
    "cat" : (lambda *args:
             "".join(args)
             ),
    "+" : (lambda *args: "({0})".format("+".join(*args))),
    "*" : (lambda *args: "({0})".format("*".join(*args))),
    "-" : (lambda a, b=None: "-{0}".format(a) if b is None else "({0}-{1})".format(a, b)),
    "/" : (lambda a, b=None: "{0}".format(a) if b is None else "({0}/{1})".format(float(a), float(b))),
}

operations_dict = {JsonLogicOperationMode.Evaluation: __operations_eval,
                   JsonLogicOperationMode.ExpressionBuilder: __operations_expressions}

def jsonLogic(tests, data_vals=None, operation_mode = None):
    global data
    # You've recursed to a primitive, stop!
    if tests is None or not isinstance(tests, dict):
        return tests

    data = data_vals or {}

    op = list(tests.keys())[0]
    values = tests[op]

    operations = operations_dict.get(operation_mode or JsonLogicOperationMode.Evaluation, None)
    if not operations:
        raise RuntimeError("Unkown operation mode: {0}".format(operation_mode))

    if op not in operations:
        raise RuntimeError("Unrecognized operation %s" % op)

    # Easy syntax for unary operators, like {"var": "x"} instead of strict
    # {"var": ["x"]}
    if type(values) not in [list, tuple]:
        values = [values]

    # Recursion!
    values = map(lambda val: jsonLogic(val, data, operation_mode), values)

    return operations[op](*values)

def load_all_rule_sets(rules_location):
    all_rulesets_files = glob(rules_location)
    if not all_rulesets_files:
        raise RuntimeError("There are no files to read for jsonlogic rules with pattern {0}".format(rules_location))
    rulesets = ConfigTree()
    for k in all_rulesets_files:
        rulesets[path.basename(k).split(".")[0]] = ConfigFactory.parse_file(k)
    return rulesets

def create_rules_monomials(tests):
    if tests is None or not isinstance(tests, dict):
        return iter([tests])
    op = tests.keys()[0]
    values = tests[op]

    if type(values) not in [list, tuple]:
        values = [values]

    values = map(lambda val: create_rules_monomials(val), values)

    if op=="or":
        all_possible_rules =  [{op:r} for k in values for r in k]
    else:
        all_possible_combinations = map(list, [k for k in product(*values)])
        all_possible_rules = [{op:k} for k in all_possible_combinations]
    return iter(all_possible_rules)

def decompose_individual_rules(tests, rules_result_prefix):
    individual_rules = create_rules_monomials(tests)
    rules = dict([("{0}_{1}".format(rules_result_prefix, x_y[0]), x_y[1]) for x_y in enumerate(individual_rules)])
    return rules

if __name__=="__main__":
    tests = {"and": [{"or": [{"==": ["a", 2]}, {"==": ["b", 3]}]}, {"or": [{"==": ["c", 4]}, {"==": ["d", 5]}]}]}
    result = create_monomials(tests)


    rules = { "and" : [
        {"<" : [ { "var" : "temp" }, 110 ]},
        {"==" : [ { "var" : "pie.filling" }, "'apple'" ] }
    ] }
    rules = ConfigFactory.from_dict(rules)

    data = { "temp" : 100, "pie" : { "filling" : "apple" } }
    print(jsonLogic(rules, data, JsonLogicOperationMode.ExpressionBuilder))
