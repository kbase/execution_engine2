# def _run_admin_command(self, command, params):
#     available_commands = ["cancel_job", "view_job_logs"]
#     if command not in available_commands:
#         raise ValueError(f"{command} not an admin command. See {available_commands} ")
#     commands = {"cancel_job": self.cancel_job, "view_job_logs": self.view_job_logs}
#     p = {
#         "cancel_job": {
#             "job_id": params.get("job_id"),
#             "terminated_code": params.get(
#                 "terminated_code", TerminatedCode.terminated_by_admin.value
#             ),
#         },
#         "view_job_logs": {"job_id": params.get("job_id")},
#     }
#     return commands[command](**p[command])
#
#     def admin_role(self, token):
#         """
#         Check to see which role the given token has
#         :param token: Token to inspect
#         :return: One of 'EE2_ADMIN_RO' or 'EE2_ADMIN` or None
#         """
#         return AdminAuthUtil(self.auth_url, self.admin_roles).get_admin_role(
#             token=token, read_role="EE2_ADMIN_RO", write_role="EE2_ADMIN"
#         )
#
#     def get_job_wrapper(self, job_id, required_admin_role=None):
#         """
#         If you are an admin, you can
#         If you are not an admin, you
#         :param job_id:
#         :return:
#         """
#         if required_admin_role is not None and required_admin_role in self.roles:
#             job = self.get_mongo_util().get_job(job_id=job_id)
#             logging.info(f"ADMIN USER has permission to cancel job {job_id}")
#             self.logger.debug(f"ADMIN USER has permission to cancel job {job_id}")
#         else:
#             job = self.get_job_with_permission(job_id, JobPermissions.WRITE)
#             logging.info(f"User has permission to cancel job {job_id}")
#             self.logger.debug(f"User has permission to cancel job {job_id}")
#         return job
#
#     def administer(self, command, params, token):
#         """
#         Run commands as an administrator. Requires a token for a user with an EE2 administrative role.
#         Currently allowed commands are cancel_job and view_job_logs.
#
#         Commands are given as strings, and their parameters are given as a dictionary of keys and values.
#         For example:
#             administer("cancel_job", {"job_id": 12345}, auth_token)
#         is the same as running
#             cancel_job(12345)
#         but with administrative privileges.
#         :param command: The command to run (See specfile)
#         :param params: The parameters for that command that will be expanded (See specfile)
#         :param token: The auth token (Will be checked for the correct auth role)
#         :return:
#         """
#         logging.info(
#             f'Attempting to run administrative command "{command}" as user {self.user_id}'
#         )
#         # set admin privs, one way or the other
#         self.is_admin = self._is_admin(token)
#         if not self.is_admin:
#             raise PermissionError(
#                 f"User {self.user_id} is not authorized to run administrative commands."
#             )
#         self._run_admin_command(command, params)
#         self.is_admin = False
