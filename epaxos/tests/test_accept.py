from base_epaxos_test import EpaxosTestCase

from epaxos.replica import *


class AcceptTest(EpaxosTestCase):

    def test_handle_accept(self):
        replicas = self.create_replicas(2)
        leader, remote = replicas

        instance = remote.get_instance(replicas)
        remote.preaccept(instance)

        remote_instance = deepcopy(instance)
        expected_deps = {uuid1(), uuid1()}
        remote_instance.dependencies = expected_deps
        remote_instance.incr_ballot()
        response = remote.handle_accept(AcceptRequest(remote_instance, []))
        self.assertIsInstance(response, AcceptResponse)
        self.assertIsNone(response.rejected_ballot)
        self.assertIsNot(remote_instance, instance)
        self.assertEqual(instance.dependencies, expected_deps)

    def test_handle_accept_ballot_failure(self):
        replicas = self.create_replicas(2)
        leader, remote = replicas

        instance = remote.get_instance(replicas)
        remote.preaccept(instance)

        remote_instance = deepcopy(instance)
        response = remote.handle_accept(AcceptRequest(remote_instance, []))
        self.assertIsInstance(response, AcceptResponse)
        self.assertEqual(response.rejected_ballot, instance.ballot)

