import logging
from enum import Enum

from cachetools import TTLCache

from execution_engine2.authorization.authstrategy import (
    can_read_job,
    can_write_job,
)
from execution_engine2.db.models.models import (
    Job,
)


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"

def get_roles(sdklmr, user_id, token):
    if user_id in self.roles_cache:
        roles = self.roles_cache.get(user_id)
    else:
        aau = AdminAuthUtil(self.auth_url, self.admin_roles)
        roles = aau.get_user_roles()
        self.roles_cache[user_id] = roles
    return roles



def get_cache(cache, size, expire):
    if cache is None:
        cache = TTLCache(
            maxsize=size,
            ttl=expire
        )
    return cache


def _update_job_permission_cache(self, job_id, user_id, level, perm):

    if not self.job_permission_cache.get(job_id):
        self.job_permission_cache[job_id] = {user_id: {level: perm}}
    else:
        job_permission = self.job_permission_cache[job_id]
        if not job_permission.get(user_id):
            job_permission[user_id] = {level: perm}
        else:
            job_permission[user_id][level] = perm

def _get_job_permission_from_cache(self, job_id, user_id, level):

    if not self.job_permission_cache.get(job_id):
        return False
    else:
        job_permission = self.job_permission_cache[job_id]
        if not job_permission.get(user_id):
            return False
        else:
            return job_permission[user_id].get(level)

def _test_job_permission_with_cache(self, job_id, permission):
    if not self._get_job_permission_from_cache(job_id, self.user_id, permission):
        job = self.get_mongo_util().get_job(job_id=job_id)
        self._test_job_permissions(job, job_id, permission)

    self.logger.debug("you have permission to {} job {}".format(permission, job_id))

def _get_job_with_permission(self, job_id, permission):
    job = self.get_mongo_util().get_job(job_id=job_id)
    if not self._get_job_permission_from_cache(job_id, self.user_id, permission):
        self._test_job_permissions(job, job_id, permission)
    return job

def _test_job_permissions(
        self, job: Job, job_id: str, level: JobPermissions
) -> bool:
    """
    Tests if the currently loaded token has the requested permissions for the given job.
    Returns True if so. Raises a PermissionError if not.
    Can also raise a RuntimeError if anything bad happens while looking up rights. This
    can be triggered from either Auth or Workspace errors.

    Effectively, this can be used the following way:
    some_job = get_job(job_id)
    _test_job_permissions(some_job, job_id, JobPermissions.READ)

    ...and continue on with code. If the user doesn't have permission, a PermissionError gets
    thrown. This can either be captured by the calling function, or allowed to propagate out
    to the user and just end the RPC call.

    :param job: a Job object to seek permissions for
    :param job_id: string - the id associated with the Job object
    :param level: string - the level to seek - either READ or WRITE
    :returns: True if the user has permission, raises a PermissionError otherwise.
    """
    if self.is_admin:  # bypass if we're in admin mode.
        self._update_job_permission_cache(job_id, self.user_id, level, True)
        return True
    try:
        perm = False
        if level == JobPermissions.READ:
            perm = can_read_job(job, self.user_id, self.token, self.config)
            self._update_job_permission_cache(job_id, self.user_id, level, perm)
        elif level == JobPermissions.WRITE:
            perm = can_write_job(job, self.user_id, self.token, self.config)
            self._update_job_permission_cache(job_id, self.user_id, level, perm)
        if not perm:
            raise PermissionError(
                f"User {self.user_id} does not have permission to {level} job {job_id}"
            )
    except RuntimeError as e:
        logging.error(
            f"An error occurred while checking permissions for job {job_id}"
        )
        raise e