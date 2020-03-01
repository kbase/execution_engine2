import logging
from enum import Enum

from cachetools import TTLCache

from execution_engine2.authorization.authstrategy import can_read_job, can_write_job
from execution_engine2.db.models.models import Job


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


def get_roles(sdklmr, user_id, token):
    if user_id in sdkmr.roles_cache:
        roles = sdkmr.roles_cache.get(user_id)
    else:
        aau = AdminAuthUtil(sdkmr.auth_url, sdkmr.admin_roles)
        roles = aau.get_user_roles()
        sdkmr.roles_cache[user_id] = roles
    return roles


def get_cache(cache, size, expire):
    if cache is None:
        cache = TTLCache(maxsize=size, ttl=expire)
    return cache


def _get_job_permission_from_cache(sdkmr, job_id, user_id, level):
    if not sdkmr.job_permission_cache.get(job_id):
        return False
    else:
        job_permission = sdkmr.job_permission_cache[job_id]
        if not job_permission.get(user_id):
            return False
        else:
            return job_permission[user_id].get(level)


def _test_job_permission_with_cache(sdkmr, job_id, permission):
    if not _get_job_permission_from_cache(sdkmr, job_id, sdkmr.user_id, permission):
        job = sdkmr.get_mongo_util().get_job(job_id=job_id)
        _test_job_permissions(job, job_id, permission)

    sdkmr.logger.debug("you have permission to {} job {}".format(permission, job_id))


def _get_job_with_permission(sdkmr, job_id, permission):
    job = sdkmr.get_mongo_util().get_job(job_id=job_id)

    if not _get_job_permission_from_cache(
        sdkmr=sdkmr, job_id=job_id, user_id=sdkmr.user_id, level=permission
    ):
        _test_job_permissions(sdkmr=sdkmr, job=job, job_id=job_id, level=permission)
    return job


def _update_job_permission_cache(sdkmr, job_id, user_id, level, perm):
    if not sdkmr.job_permission_cache.get(job_id):
        sdkmr.job_permission_cache[job_id] = {user_id: {level: perm}}
    else:
        job_permission = sdkmr.job_permission_cache[job_id]
        if not job_permission.get(user_id):
            job_permission[user_id] = {level: perm}
        else:
            job_permission[user_id][level] = perm


def _test_job_permissions(sdkmr, job: Job, job_id: str, level: JobPermissions) -> bool:
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

    if sdkmr.is_admin:  # bypass if we're in admin mode.
        _update_job_permission_cache(sdkmr, job_id, sdkmr.user_id, level, True)
        return True
    try:
        perm = False
        if level.value == JobPermissions.READ.value:

            perm = can_read_job(job, sdkmr.user_id, sdkmr.token, sdkmr.config)
            _update_job_permission_cache(sdkmr, job_id, sdkmr.user_id, level, perm)
        elif level.value == JobPermissions.WRITE.value:

            perm = can_write_job(job, sdkmr.user_id, sdkmr.token, sdkmr.config)
            _update_job_permission_cache(sdkmr, job_id, sdkmr.user_id, level, perm)
        else:
            raise PermissionError(
                f"Please provide a valid level of permissions  {level}"
            )
        if not perm:
            raise PermissionError(
                f"User {sdkmr.user_id} does not have permission to {level} job {job_id}"
            )
    except RuntimeError as e:
        logging.error(f"An error occurred while checking permissions for job {job_id}")
        raise e
