from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, Any, List, Optional


class JobExecutorSingleton(type):
    """
    A metaclass for the Job
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super(JobExecutorSingleton, cls).__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class JobExecutor(metaclass=JobExecutorSingleton):
    """
    A static class that provides a global ThreadPoolExecutor instance to accept and execute jobs.
    """

    _executor: Optional[ThreadPoolExecutor] = None
    _jobs: List[Future] = []

    @classmethod
    def initialize_executor(cls, max_workers: int = 3) -> None:
        """
        Initialize the ThreadPoolExecutor with a specified number of workers.

        :param max_workers: The number of threads that the executor should run.
        """
        if cls._executor is None:
            cls._executor = ThreadPoolExecutor(max_workers=max_workers)
            print(f"Executor initialized with {max_workers} workers.")

    @classmethod
    def submit_job(cls, job: Callable[..., Any], *args: Any, **kwargs: Any) -> Future:
        """
        Submit a job to the ThreadPoolExecutor for asynchronous execution.

        :param job: A callable that represents the job to be executed.
        :param args: Positional arguments to pass to the job.
        :param kwargs: Keyword arguments to pass to the job.
        :return: A Future object representing the execution of the job.
        """
        if cls._executor is None:
            raise RuntimeError(
                "JobExecutor must be initialized before submitting jobs."
            )

        future = cls._executor.submit(job, *args, **kwargs)
        cls._jobs.append(future)
        print(f"Job submitted: {job.__name__} with args: {args}, kwargs: {kwargs}")
        return future

    @classmethod
    def get_completed_jobs(cls) -> List[Any]:
        """
        Get the results of all completed jobs.

        :return: A list of results from completed jobs.
        """
        completed_jobs = []
        for job in cls._jobs:
            try:
                result = job.result()
                completed_jobs.append(result)
            except Exception as e:
                print(f"Job resulted in an error: {e}")
        return completed_jobs

    @classmethod
    def shutdown_executor(cls, wait: bool = True) -> None:
        """
        Shutdown the executor, optionally waiting for all jobs to complete.

        :param wait: If True, wait for all pending jobs to complete before shutting down.
        """
        if cls._executor:
            cls._executor.shutdown(wait=wait)
            cls._executor = None
            print("Executor shut down.")


if __name__ == '__main__':
    import time

    def example_job(job_id: int) -> str:
        """Simulate a job by sleeping and returning a result."""
        print(f"Job {job_id} started.")
        time.sleep(2)
        print(f"Job {job_id} finished.")
        return f"Result of job {job_id}"

    JobExecutor.initialize_executor(max_workers=3)

    for i in range(5):
        JobExecutor.submit_job(example_job, i)

    results = JobExecutor.get_completed_jobs()
    print(f"All jobs completed with results: {results}")

    JobExecutor.shutdown_executor()
