import os, sys, subprocess
import pandas as pd







if __name__=="__main__":
    python_bin = "C:/Users/JackKim/AppData/Local/conda/conda/envs/bigdataml/python"
    script = "C:/DCM/dcm-intuition/processes/data_processing/calibrations/quantamental_ml/gan_py3/gan_util_functions.py"
    command = [python_bin, script, sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    for line in iter(p.stdout.readline, ''):
        print(line.rstrip())
    retval = p.wait()
    
    print("done")