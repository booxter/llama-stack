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

# TODO: testing: add unit tests for the scheduler module
# TODO: testing: introduce a stub provider, implement unit and e2e tests using it

# TODO: introduce a new global API for job management (including client bindings)
# TODO: move the module to a common space and consider how jobs are aggregated
#       between different schedulers

# TODO: add type hints everywhere

# TODO: reconcile with API level options
# TODO: use a library to maintain state machine transitions?
# TODO: do we need any other states? (e.g. cancelled)
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
    def __init__(self, job_type: str, handler, deps: list[JobDependency] | None = None):
        super().__init__()
        # TODO: does id even belong here? should it be in scheduler?
        self.id: JobID | None = None
        # TODO: validate job_type (enum?)
        self.type = job_type
        self._handler = handler
        self._deps = deps or []
        self._artifacts: list[JobArtifact] = []
        self.logs: list[str] = []
        # TODO: track states in scheduler?
        self._state_transitions: list[JobStateTransition] = [(datetime.now(), JobStatus.new)]

    # Defines dependencies to fulfill to be able to execute the job
    # These could be hardware resources; desired states for other jobs; etc.
    # The scheduler will consider these constraints when deciding whether a
    # job is ready to execute.
    @property
    def deps(self) -> list[JobDependency]:
        return self._deps[:]

    # called by user or scheduler (for non-resumable jobs?)
    def cancel(self):
        if self.status in _COMPLETED_STATUSES:
            raise ValueError("Job is already completed, cannot cancel")
        self.status = JobStatus.failed # TODO: add cancelled status?

    # TODO: add abstract handler interface
    @property
    def handler(self):
        return self._handler

    # TODO: status should probably be a scheduler thing
    @property
    def status(self) -> JobStatus:
        return self._state_transitions[-1][1]

    @status.setter
    def status(self, status: JobStatus):
        if status in _COMPLETED_STATUSES and self.status in _COMPLETED_STATUSES:
            # TODO: raise or just ignore?
            raise ValueError(f"Job is already in a completed state ({self.status})")
        if self.status == status:
            return
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
        return self._find_state_transition_date(_COMPLETED_STATUSES)


# TODO: should it be an abstract interface?
class SchedulerBackend:
    # TODO: dump logs to disc?
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


# TODO: we should consider 3rd party libraries for alternative backends (celery?)
_BACKENDS = {
    "naive": NaiveSchedulerBackend,
}


# TODO: add tracing capabilities
class Scheduler:
    def __init__(self, backend: str = "naive"):
        # TODO: restore from disc at startup
        # TODO: resume jobs that were scheduled / running before the shutdown
        # TODO: access jobs through a persisting facade
        self._jobs: dict[JobID, Job] = {}
        # TODO: add a way to switch backends (via config?)
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
        job.logs.append(message) # and/or push to external storage?

    def _on_status_change_cb(self, job, status):
        job.status = status

    def _on_artifact_collected_cb(self, job, name, type_, uri, metadata):
        job.register_artifact(name, type_, uri, metadata)

    # called by provider to add job to queue
    def schedule(self, job: Job, job_uuid: JobID | None = None) -> JobID:
        job_uuid = job_uuid or str(uuid.uuid4())
        job.id = job_uuid
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
        # TODO: actually stop the job handler async task
        self.get_job(job_uuid).cancel()

    def get_job(self, job_uuid) -> Job:
        try:
            return self._jobs[job_uuid]
        except KeyError:
            raise ValueError(f"Job {job_uuid} not found")

    # TODO: this will require a streaming implementation: block for new messages
    # TODO: do we start from the first message or from the end (only new
    #       messages), or last X and then wait for more?
    def tail(self, job_uuid):
        yield from self.get_job(job_uuid).logs

    # TODO: implement it in API, make sure it works
    # TODO: clean up artifacts from disc too
    def delete(self, job_uuid):
        job = self.get_job(job_uuid)
        if job.status not in _COMPLETED_STATUSES:
            raise ValueError(f"Job {job_uuid} is not completed, cannot delete")
        # TODO: is it racy?..
        del self._jobs[job_uuid]

    # TODO: return complete jobs? or just more stuff?
    def get_jobs(self) -> list[Job]:
        print("Getting jobs")
        return list(self._jobs.values())

    def get_artifacts(self, job_uuid) -> list[JobArtifact]:
        return self.get_job(job_uuid).artifacts
