from base_epaxos_test import EpaxosTestCase

from epaxos.replica import *

class PreacceptTest(EpaxosTestCase):

    def test_replica_leader_instance(self):
        replicas = self.create_replicas(3)
        leader = replicas[0]
        remote = replicas[1:]
        instance = leader.get_instance(replicas)

        leader.preaccept(instance)

        self.assertIn(instance.iid, leader.instances)
        self.assertEqual(instance.dependencies, set())
        for replica in remote:
            self.assertIn(instance.iid, replica.instances)
            self.assertEqual(replica.instances[instance.iid].dependencies, set())

    def test_non_replica_leader_instance(self):
        replicas = self.create_replicas(3)
        leader = self.create_replicas(1)[0]
        instance = leader.get_instance(replicas)

        leader.preaccept(instance)

        self.assertNotIn(instance.iid, leader.instances)

        # shouldn't be anything until after accept
        self.assertIsNone(instance.dependencies)
        for replica in replicas:
            self.assertIn(instance.iid, replica.instances)
            self.assertEqual(replica.instances[instance.iid].dependencies, set())

    def test_handle_preaccept_agreeing_deps(self):
        replicas = self.create_replicas(2)
        leader, remote = replicas
        committed = remote.get_instance(replicas)
        committed.commit()
        remote.instances[committed.iid] = committed

        expected_deps = remote.current_deps()
        self.assertEqual(expected_deps, {committed.iid})

        instance = remote.get_instance(replicas)
        instance.state = Instance.State.PREACCEPTED
        instance.dependencies = {committed.iid}

        response = remote.handle_preaccept(PreacceptRequest(instance))
        self.assertIsInstance(response, PreacceptResponse)
        remote_instance = remote.instances[instance.iid]
        self.assertIsNot(remote_instance, instance)
        self.assertEqual(remote_instance.dependencies, expected_deps)

    def test_handle_preaccept_disagreeing_deps(self):
        replicas = self.create_replicas(2)
        leader, remote = replicas
        committed = remote.get_instance(replicas)
        committed.commit()
        remote.instances[committed.iid] = committed

        expected_deps = remote.current_deps()
        self.assertEqual(expected_deps, {committed.iid})

        instance = remote.get_instance(replicas)
        instance.state = Instance.State.PREACCEPTED
        instance.dependencies = set()

        response = remote.handle_preaccept(PreacceptRequest(instance))
        self.assertIsInstance(response, PreacceptResponse)
        remote_instance = remote.instances[instance.iid]
        self.assertIsNot(remote_instance, instance)
        self.assertEqual(remote_instance.dependencies, expected_deps)

class PreacceptIntegrationTest(EpaxosTestCase):
    pass