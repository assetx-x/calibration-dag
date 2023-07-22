from calibration_pipelines import *



if __name__=="__main__":
    # arguments
    # [config_name] [target_dt]
    num_args = len(sys.argv)
    if num_args<2:
        print("ERROR: Arguments misspecified")
        print("[config_name] [target_dt](optional - if not specified defaulted to now)")
    else:
        config_name = sys.argv[1]
        start_dt = pd.Timestamp("2000-01-03")
        base_file = os.path.join(os.environ["QML_CONFIG_DIR"], config_name)
        
        if num_args>2:
            target_dt = pd.Timestamp(sys.argv[2])
        else:
            target_dt = pd.Timestamp.now().normalize()

        flow_parameters = BasicTaskParameters(target_dt, start_dt, target_dt, target_dt, TaskflowPipelineRunMode.Calibration)
        
        print("*******************************************************")
        print("*******************************************************")
        print("start_dt : {0}".format(start_dt))
        print("target_dt : {0}".format(target_dt))
        print("config : {0}".format(base_file))
        print("*******************************************************")
        print("*******************************************************")
        
        pipeline_desc = TaskflowPipelineDescription(base_file)
        pipeline_runner = TaskflowPipelineRunner(pipeline_desc)
        pipeline_runner.run(flow_parameters)        