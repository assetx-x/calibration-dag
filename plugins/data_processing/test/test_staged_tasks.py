import os

os.environ['POSTGRES_USER'] = 'test_user'
os.environ['POSTGRES_PWD'] = 'test_password'

from base import StageResultTarget


class ConnectionMock(object):
    def __init__(self, results):
        self.results = results

    def __call__(self, *args, **kwargs):
        return self

    def cursor(self):
        return self

    def execute(self, *args, **kwargs):
        pass

    def fetchall(self):
        return self.results


def test_stage_result_should_not_complete_no_results():
    target = StageResultTarget("test_stage_id", treat_failed_as_completed=True)
    target.connect = ConnectionMock([])
    assert not target.exists(), "Target should not be completed when stages are empty"


def test_stage_result_should_complete_if_some_failed():
    target = StageResultTarget("test_stage_id", treat_failed_as_completed=True)
    target.connect = ConnectionMock([("test_stage_id", "Success"), ("test_stage_id", "Not_Tested")])
    assert target.exists(), "Target should be completed when some steps failed and treat_failed_as_completed=True"


def test_stage_result_should_complete_if_all_failed():
    target = StageResultTarget("test_stage_id", treat_failed_as_completed=True)
    target.connect = ConnectionMock([("test_stage_id", "Failed"), ("test_stage_id", "Not_Tested")])
    assert target.exists(), "Target should be completed when all steps failed and treat_failed_as_completed=True"


def test_stage_result_should_not_complete_no_results_success_only():
    target = StageResultTarget("test_stage_id", treat_failed_as_completed=False)
    target.connect = ConnectionMock([])
    assert not target.exists(), "Target should not be completed when stages are empty"


def test_stage_result_should_not_complete_if_some_stages_failed():
    target = StageResultTarget("test_stage_id", treat_failed_as_completed=False)
    target.connect = ConnectionMock([("test_stage_id", "Success"), ("test_stage_id", "Failed")])
    assert not target.exists(), "Target should not be completed when some steps failed and treat_failed_as_completed=False"


def test_stage_result_should_not_complete_if_some_stages_not_tested():
    target = StageResultTarget("test_stage_id", treat_failed_as_completed=False)
    target.connect = ConnectionMock([("test_stage_id", "Success"), ("test_stage_id", "Not_Tested")])
    assert not target.exists(), "Target should not be completed when some steps were not tested and treat_failed_as_completed=False"


def test_stage_result_should_not_complete_if_all_stages_failed():
    target = StageResultTarget("test_stage_id", treat_failed_as_completed=False)
    target.connect = ConnectionMock([("test_stage_id", "Failed"), ("test_stage_id", "Failed")])
    assert not target.exists(), "Target should not be completed when all steps failed and treat_failed_as_completed=False"


def test_stage_result_should_complete_if_all_stages_succeeded():
    target = StageResultTarget("test_stage_id", treat_failed_as_completed=False)
    target.connect = ConnectionMock([("test_stage_id", "Success"), ("test_stage_id", "Success")])
    assert target.exists(), "Target should be completed when all steps succeeded and treat_failed_as_completed=False"
