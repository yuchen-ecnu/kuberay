import unittest

from migrate_api import migrated_spec, runtime_targets


class MigrationTest(unittest.TestCase):
    def test_paths_and_runtime_targets(self):
        for autoscale in (False, True):
            frc = {"metadata": {"uid": "f"}, "spec": {
                "enableInTreeAutoscaling": autoscale,
                "autoscalerOptions": {"version": "v2"} if autoscale else None,
                "primaryCluster": {"workerGroups": [{"groupName": "local", "replicas": 1}]},
                "memberClusters": [{"workerGroups": [{"groupName": "remote", "replicas": 0}]}]}}
            primary = {"metadata": {"ownerReferences": [{"controller": True, "uid": "f"}]},
                       "spec": {"workerGroupSpecs": [{"groupName": "local", "replicas": 3},
                                                     {"groupName": "remote", "replicas": 4,
                                                      "scaleStrategy": {"workersToDelete": ["selected"]}}]}}
            spec = migrated_spec(frc, primary)
            self.assertNotIn("enableInTreeAutoscaling", spec)
            self.assertNotIn("autoscalerOptions", spec)
            self.assertEqual(spec["primaryCluster"]["enableInTreeAutoscaling"], autoscale)
            self.assertEqual(spec["primaryCluster"]["autoscalerOptions"], {"version": "v2"} if autoscale else None)
            self.assertEqual(spec["primaryCluster"]["workerGroups"][0]["replicas"], 1)
            self.assertEqual(spec["memberClusters"][0]["workerGroups"][0]["replicas"], 0)
            self.assertEqual(frc["spec"]["memberClusters"][0]["workerGroups"][0]["replicas"], 0)
            primary["metadata"]["ownerReferences"][0]["uid"] = "foreign"
            with self.assertRaises(ValueError):
                migrated_spec(frc, primary)

    def test_conflicting_paths_are_rejected(self):
        frc = {"metadata": {"uid": "f"}, "spec": {"enableInTreeAutoscaling": True,
                "primaryCluster": {"enableInTreeAutoscaling": False}}}
        primary = {"metadata": {"ownerReferences": [{"controller": True, "uid": "f"}]}}
        with self.assertRaises(ValueError):
            migrated_spec(frc, primary)

    def test_already_nested_does_not_change_autoscaled_targets(self):
        frc = {"metadata": {"uid": "f"}, "spec": {
            "primaryCluster": {"enableInTreeAutoscaling": True,
                               "autoscalerOptions": {"version": "v2"},
                               "workerGroups": [{"groupName": "local", "replicas": 0}]},
            "memberClusters": []}}
        primary = {"metadata": {"ownerReferences": [{"controller": True, "uid": "f"}]},
                   "spec": {"workerGroupSpecs": [{"groupName": "local", "replicas": 2}]}}
        self.assertEqual(migrated_spec(frc, primary), frc["spec"])

    def test_runtime_target_checkpoint(self):
        primary = {"spec": {"workerGroupSpecs": [{"groupName": "remote",
            "managedBy": "ray.io/federated-raycluster-controller", "replicas": 2,
            "scaleStrategy": {"workersToDelete": ["worker-1"]}}]}}
        saved = runtime_targets(primary)
        primary["spec"]["workerGroupSpecs"][0]["replicas"] = 3
        self.assertNotEqual(runtime_targets(primary), saved)
        primary["spec"]["workerGroupSpecs"][0]["replicas"] = 2
        primary["spec"]["workerGroupSpecs"][0]["scaleStrategy"]["workersToDelete"].clear()
        self.assertNotEqual(runtime_targets(primary), saved)


if __name__ == "__main__":
    unittest.main()
