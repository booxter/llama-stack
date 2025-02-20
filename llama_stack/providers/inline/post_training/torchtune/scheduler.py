import asyncio
from datetime import datetime
from enum import Enum
import functools
import threading
from typing import Any, Dict, Tuple, Iterable
import uuid


type JobDependency = Dict[str, Any] # TBD exact shape
type JobArtifact = Dict[str, Any] # TBD exact shape
type JobID = str # TBD exact shape
type JobStateTransition = Tuple[datetime, JobStatus]

# TODO: add type hints everywhere

# TODO: we should consider 3rd party libraries for scheduling (celery?) and job
# state machine


# TODO: reconcile with API level options
class JobStatus(Enum):
  new = "new"
  scheduled = "scheduled"
  running = "running"
  paused = "paused"
  failed = "failed"
  completed = "completed"


_COMPLETED_STATUSES = {JobStatus.completed, JobStatus.failed}


class Job:
    # TODO: add type hint for handler callable
    def __init__(self, handler, deps: list[JobDependency] | None = None):
        super().__init__()
        self._handler = handler
        self._deps = deps or []
        self._artifacts: list[JobArtifact] = []
        # TODO: track states in scheduler
        self._state_transitions: list[JobStateTransition] = []
        self.status = JobStatus.new

    # Defines dependencies to fulfill to be able to execute the job
    # These could be hardware resources; desired states for other jobs; etc.
    # The scheduler will consider these constraints when deciding whether a
    # job is ready to execute.
    @property
    def deps(self) -> list[JobDependency]:
        return self._deps[:]

    # called by user or scheduler (for non-resumable jobs?)
    def cancel(self):
        raise NotImplementedError()

    # TODO: add abstract handler interface
    @property
    def handler(self):
        return self._handler

    # TODO: status should probably be a scheduler thing
    @property
    def status(self) -> JobStatus:
        return self._status

    @status.setter
    def status(self, status: JobStatus):
        self._status = status
        self._state_transitions.append((datetime.now(), status))

    @property
    def artifacts(self) -> list[JobArtifact]:
        return self._artifacts

    def register_artifact(self, name, type_, uri, metadata):
        self._artifacts.append({
            "name": name,
            "type": type_,
            "uri": uri,
            "metadata": metadata
        })

    def _find_state_transition_date(self, status: Iterable[JobStatus]) -> datetime | None:
        for date, s in reversed(self._state_transitions):
            if s in status:
                return date

    @property
    def scheduled_at(self) -> datetime | None:
        return self._find_state_transition_date([JobStatus.scheduled])

    @property
    def started_at(self) -> datetime | None:
        return self._find_state_transition_date([JobStatus.running])

    @property
    def completed_at(self) -> datetime | None:
        if self._status in _COMPLETED_STATUSES:
            return self._find_state_transition_date(_COMPLETED_STATUSES)


# TODO: should it be an abstract interface?
class SchedulerBackend:
    def _on_log_message_cb(self, job, message):
        pass

    def _on_status_change_cb(self, job, status):
        pass

    def _on_artifact_collected_cb(self, job, name, type_, uri, metadata):
        pass



class NaiveSchedulerBackend(SchedulerBackend):
    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    # TODO: actually plug it in; also handle signals
    def shutdown(self):
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    def schedule(self, job: Job):
        asyncio.run_coroutine_threadsafe(
            job.handler(
                functools.partial(self._on_log_message_cb, job),
                functools.partial(self._on_status_change_cb, job),
                functools.partial(self._on_artifact_collected_cb, job)
            ), self._loop)


_BACKENDS = {
    "naive": NaiveSchedulerBackend,
}


# TODO: add tracing capabilities
class Scheduler:
    def __init__(self, backend: str = "naive"):
        self._jobs: dict[JobID, Job] = {}
        self._backend = _BACKENDS[backend]()

        # TODO: Wrap instead of replacing
        self._backend._on_log_message_cb = self._on_log_message_cb
        self._backend._on_status_change_cb = self._on_status_change_cb
        self._backend._on_artifact_collected_cb = self._on_artifact_collected_cb

    def _on_log_message_cb(self, job, message):
        # this will be called whenever the Job handler calls on_log_message_cb.
        # We can then retain the passed message, perhaps store in a file, print to
        # screen, push to external storage or whatever else provider may want to do with
        # it. Later, when the job is complete, the collected logs will be one of the
        # artifacts that will be returned through API.
        # TODO: actually do something with the logs
        pass

    def _on_status_change_cb(self, job, status):
        job.status = status

    def _on_artifact_collected_cb(self, job, name, type_, uri, metadata):
        job.register_artifact(name, type_, uri, metadata)

    # called by provider to add job to queue
    def schedule(self, job: Job, job_uuid: JobID | None = None) -> JobID:
        job_uuid = job_uuid or str(uuid.uuid4())

        print(f"Scheduling job {job_uuid}")
        self._jobs[job_uuid] = job
        job.status = JobStatus.scheduled
        print(f"Scheduled job {job_uuid}")

        # TODO: untangle schedule api from execution (e.g. need to handle job deps)
        print("Running job", job_uuid)
        self._backend.schedule(job)
        job.status = JobStatus.running
        print("Job is running now", job_uuid)

        return job_uuid

    def cancel(self, job_uuid):
        raise NotImplementedError()

    def get_job(self, job_uuid) -> Job:
        try:
            return self._jobs[job_uuid]
        except KeyError:
            raise ValueError(f"Job {job_uuid} not found")

    def tail(self, job_uuid):
        raise NotImplementedError()

    def delete(self, job_uuid):
        raise NotImplementedError()

    # TODO: return complete jobs? or just more stuff?
    def get_jobs(self) -> list[JobID]:
        print("Getting jobs")
        return list(self._jobs.keys())

    def get_artifacts(self, job_uuid) -> list[JobArtifact]:
        return self._jobs[job_uuid].artifacts
