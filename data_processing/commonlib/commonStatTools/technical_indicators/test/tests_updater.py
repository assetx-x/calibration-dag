import os
import sys
import random
import time
from collections import OrderedDict

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from online import (
    OnlineRSI,
    OnlineSMA,
    OnlineEWMA,
    OnlineKAMA,
    OnlineMACD,
    OnlineOscillator,
    OnlineROC,
    OnlineMomentum,
    OnlineDisparity,
    OnlineStochasticK,
    OnlineStochasticD,
    OnlineSlowStochasticD,
    OnlineTSI
)


MAX_PRICE = 1000


def perf_online_indicator(indicator_class, steps, *args, **kwargs):
    scale_result = ()
    rate = None

    for count in steps:
        start_time = time.clock()
        indicator = indicator_class("SYM", *args, **kwargs)
        random.seed()
        for i in xrange(count):
            next_price = random.random() * MAX_PRICE
            indicator.update(next_price)
        end_time = time.clock()
        time_delta = end_time - start_time
        scale_result += (time_delta,)
        rate = float(count) / time_delta


    return scale_result, rate


steps = (12500, 25000, 50000)
indicators_to_perf = OrderedDict((
    (OnlineRSI, (100, )),
    (OnlineSMA, (100, )),
    (OnlineEWMA, (100, )),
    (OnlineKAMA, ()),
    (OnlineMACD, ()),
    (OnlineOscillator, ()),
    (OnlineROC, ()),
    (OnlineMomentum, ()),
    (OnlineDisparity, ()),
    (OnlineStochasticK, ()),
    (OnlineStochasticD, ()),
    (OnlineSlowStochasticD, ()),
    (OnlineTSI, ())
))

for indicator_class, args in indicators_to_perf.iteritems():
    scale, rate = perf_online_indicator(indicator_class, steps, *args)
    print indicator_class.__name__
    print " rate = %.2f (op/s)" % rate
    for count, delta in zip(steps, scale):
        print " count(%d) = %.2f (s)" % (count, delta)
