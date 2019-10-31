def test_get_client_groups(self):
    runner = self.getRunner()

    client_groups = runner._get_client_groups(
        "kb_uploadmethods.import_sra_from_staging"
    )

    expected_groups = "kb_upload"  # expected to fail if CI catalog is updated
    self.assertCountEqual(expected_groups, client_groups)
    client_groups = runner._get_client_groups("MEGAHIT.run_megahit")
    self.assertEqual(0, len(client_groups))

    with self.assertRaises(ValueError) as context:
        runner._get_client_groups("kb_uploadmethods")

    self.assertIn("unrecognized method:", str(context.exception.args))

def test_get_module_git_commit(self):
    runner = self.getRunner()

    git_commit_1 = runner._get_module_git_commit("MEGAHIT.run_megahit", "2.2.1")
    self.assertEqual(
        "048baf3c2b76cb923b3b4c52008ed77dbe20292d", git_commit_1
    )  # TODO: works only in CI

    git_commit_2 = runner._get_module_git_commit("MEGAHIT.run_megahit")
    self.assertTrue(isinstance(git_commit_2, str))
    self.assertEqual(len(git_commit_1), len(git_commit_2))
    self.assertNotEqual(git_commit_1, git_commit_2)