import random
import sys
import time
from unittest import skip
from unittest.case import SkipTest
from base_epaxos_test import EpaxosTestCase

from epaxos.replica import *

IS = Instance.State
RS = Replica.State

class EpaxosIntegrationTest(EpaxosTestCase):

    replication_factor = None
    leader_is_replica = None

    @classmethod
    def setUpClass(cls):
        super(EpaxosIntegrationTest, cls).setUpClass()
        if cls.replication_factor is None:
            raise SkipTest
        else:
            assert cls.replication_factor > 2

        assert cls.leader_is_replica is not None

    def setUp(self):
        super(EpaxosIntegrationTest, self).setUp()
        self.nodes = []
        self.replicas = self.create_replicas(self.replication_factor, replica_list=self.nodes)
        self.next_leader = 0

        seed = sys.argv[sys.argv.index('-seed') + 1] if '-seed' in sys.argv else str(int(time.time()))
        print "seed:", seed
        random.seed(seed)

    @property
    def N(self):
        return self.replication_factor

    @property
    def F(self):
        f = self.N / 2
        assert ((2 * f) + 1) == self.N
        return f

    @property
    def fast_path_quorum_size(self):
        return self.F + ((self.F + 1) / 2)

    @property
    def quorum_size(self):
        return self.F + 1

    def get_leader(self):
        if self.leader_is_replica:
            leader = self.replicas[self.next_leader]
            self.next_leader += 1
            self.next_leader %= len(self.replicas)
            return leader
        else:
            return self.create_replicas(1, replica_list=self.nodes)[0]

    def set_replica_state(self, replicas, state):
        for replica in replicas:
            replica.state = state

    def check_accept_phase(self, instance, replicas, skipped=False):
        func = self.assertIsNone if skipped else self.assertIsNotNone
        for replica in replicas:
            msg = "expected accept phase to " + ("not " if skipped else "") + "be run"
            func(replica.instances[instance.iid].accept_deps, msg)

    def check_replicas_instance(self, instance, expected_deps, replicas, state=None):
        expected_deps = {d.iid if isinstance(d, Instance) else d for d in expected_deps}
        for i, replica in enumerate(replicas):
            self.assertIn(instance.iid, replica.instances)
            replica_instance = replica.instances[instance.iid]
            self.assertEqual(expected_deps, replica_instance.dependencies, "{}: {} != {}".format(i, expected_deps, replica_instance.dependencies))
            if state is not None:
                if isinstance(state, (list, tuple)):
                    self.assertIn(replica_instance.state, state, "{}: {} not in {}".format(i, [IS.to_string(s) for s in state], IS.to_string(replica_instance.state)))
                else:
                    self.assertEqual(state, replica_instance.state, "{}: {} != {}".format(i, IS.to_string(state), IS.to_string(replica_instance.state)))

    def check_replicas_instance_unseen(self, instance, replicas):
        for replica in replicas:
            self.assertNotIn(instance.iid, replica.instances)

    def check_execution_order(self, replicas, expected_order=None, allow_unexecuted=False):
        """
        checks that the execution order of each replica is the same

        if an expected_order kwarg is provided, that's used to check against,
        otherwise, it ensures that the execution order is the same across all
        replicas.

        if allow_unexecuted is True, it's ok if all replicas haven't executed all instances,
        their execution order will only be evaluated up to the most recently executed instance
        """
        if expected_order is not None:
            expected_order = [i.iid if isinstance(i, Instance) else i for i in expected_order]
        else:
            expected_order = [r.executed for r in replicas]
            max_len = max([len(e) for e in expected_order])
            expected_order = [e for e in expected_order if len(e) == max_len][0]

        placeholder = '         -- not executed --         '
        differences = False
        msg = []
        for replica in replicas:
            expected = expected_order
            actual = replica.executed

            msg.append('\n' + str(replica))
            for i in range(max(len(expected), len(actual))):
                e = expected[i] if i < len(expected) else None
                a = actual[i] if i < len(actual) else None

                line_msg = "{} | {}".format(e or placeholder, a or placeholder)
                if e != a and not (allow_unexecuted and None in [e, a]):
                    line_msg += " <--"
                    differences = True
                msg.append(line_msg)

        if differences:
            raise AssertionError('Inconsistent execution order\n' + '\n'.join(msg))

    def test_success_case(self):
        """ all nodes reply """
        leader1 = self.get_leader()
        leader1.coordinate_request(self.replicas)
        instance1 = leader1.last_created_instance
        expected_deps = set()

        self.check_replicas_instance(instance1, expected_deps, self.replicas)
        self.check_accept_phase(instance1, self.replicas, skipped=True)

        leader2 = self.get_leader()
        leader2.coordinate_request(self.replicas)
        instance2 = leader2.last_created_instance
        expected_deps.add(instance1.iid)

        self.check_replicas_instance(instance2, expected_deps, self.replicas)
        self.check_accept_phase(instance2, self.replicas, skipped=True)

        expected_order = [instance1, instance2]
        self.check_execution_order(self.replicas, expected_order)

        if not self.leader_is_replica:
            self.assertEqual(leader1.instances, {})
            self.assertEqual(leader2.instances, {})

    def test_quorum_success_case(self):
        """ only a quorum of nodes reply """
        self.set_replica_state(self.replicas[self.quorum_size:], RS.DOWN)
        expect_accept = self.fast_path_quorum_size > self.quorum_size

        leader1 = self.get_leader()
        leader1.coordinate_request(self.replicas)
        instance1 = leader1.last_created_instance
        expected_deps = set()

        self.check_replicas_instance(instance1, expected_deps, self.replicas[:self.quorum_size])
        self.check_accept_phase(instance1, self.replicas[:self.quorum_size], skipped=(not expect_accept))

        leader2 = self.get_leader()
        leader2.coordinate_request(self.replicas)
        instance2 = leader2.last_created_instance
        expected_deps.add(instance1.iid)

        self.check_replicas_instance(instance2, expected_deps, self.replicas[:self.quorum_size])
        self.check_accept_phase(instance2, self.replicas[:self.quorum_size], skipped=(not expect_accept))

        # check unresponsive nodes don't know about instances
        for replica in self.replicas[self.quorum_size:]:
            self.assertEqual({}, replica.instances)

        expected_order = [instance1, instance2]
        self.check_execution_order(self.replicas[:self.quorum_size], expected_order)

        # TODO: check instance was executed

        if not self.leader_is_replica:
            self.assertEqual(leader1.instances, {})
            self.assertEqual(leader2.instances, {})

    def test_fast_quorum_success_case(self):
        """ only a fast quorum of nodes reply """
        self.set_replica_state(self.replicas[self.fast_path_quorum_size:], RS.DOWN)

        leader1 = self.get_leader()
        leader1.coordinate_request(self.replicas)
        instance1 = leader1.last_created_instance
        expected_deps = set()

        self.check_replicas_instance(instance1, expected_deps, self.replicas[:self.fast_path_quorum_size])
        self.check_accept_phase(instance1, self.replicas[:self.fast_path_quorum_size], skipped=True)

        leader2 = self.get_leader()
        leader2.coordinate_request(self.replicas)
        instance2 = leader2.last_created_instance
        expected_deps.add(instance1.iid)

        self.check_replicas_instance(instance2, expected_deps, self.replicas[:self.fast_path_quorum_size])
        self.check_accept_phase(instance2, self.replicas[:self.fast_path_quorum_size], skipped=True)

        # check unresponsive nodes don't know about instances
        for replica in self.replicas[self.fast_path_quorum_size:]:
            self.assertEqual({}, replica.instances)

        if not self.leader_is_replica:
            self.assertEqual(leader1.instances, {})
            self.assertEqual(leader2.instances, {})

    def test_inferred_fast_path_failure_recovery(self):
        """
        :return:
        """
        # first, preaccept an instance on a fast path minority of the nodes
        self.set_replica_state(self.replicas[:self.fast_path_quorum_size], RS.DOWN)
        self.next_leader = len(self.replicas) - 1
        leader1 = self.get_leader()
        with self.assertRaises(QuorumFailure):
            leader1.coordinate_request(self.replicas, nickname="failed_instance")
        failed_instance = leader1.last_created_instance
        self.check_replicas_instance(failed_instance, set(), self.replicas[self.fast_path_quorum_size:], IS.PREACCEPTED)
        self.check_replicas_instance_unseen(failed_instance, self.replicas[:self.fast_path_quorum_size])

        # second, a fast path of replicas responds with identical attributes, but the fast path minority responds with
        # non matching attributes. The leader doesn't receive the dissenting responses, resulting in a fast path commit,
        # but the non-leaders don't receive the commit message
        self.next_leader = 0
        leader2 = self.get_leader()
        self.set_replica_state(self.replicas, RS.UP)
        self.set_replica_state(self.replicas[self.fast_path_quorum_size:], RS.NORESPONSE)
        def second_step_commit_hook():
            self.set_replica_state(self.replicas[1:], RS.DOWN)
        leader2.coordinate_request(self.replicas, commit_hook=second_step_commit_hook, nickname="committed_instance")
        committed_instance = leader2.last_created_instance
        committed_instance_deps = set()
        # 1st instance should be committed
        self.check_replicas_instance(committed_instance, committed_instance_deps, self.replicas[:1], state=IS.EXECUTED)

        # 2nd through fast path quorum should be preaccepted with no deps
        self.check_replicas_instance(committed_instance, set(), self.replicas[1:self.fast_path_quorum_size], state=IS.PREACCEPTED)

        # fast path minority should be preaccepted with failed instance dep
        self.check_replicas_instance(committed_instance, {failed_instance.iid}, self.replicas[self.fast_path_quorum_size:], state=IS.PREACCEPTED)

        # third, switch the partition, so all nodes that contributed to the second commit are down, with the exception
        # of one which didn't commit, and run an instance. When this instance is executed, the uncommitted instance from
        # the previous run will be prepared, and the prepare stage should be able to infer that the second instance was
        # committed on the fast path, and shouldn't restart the preaccept phase
        self.set_replica_state(self.replicas, RS.DOWN)
        self.set_replica_state(self.replicas[-self.fast_path_quorum_size:], RS.UP)
        leader1.coordinate_request(self.replicas, nickname="successful_instance")
        successful_instance = leader1.last_created_instance

        # instance was committed on replica 0 with no deps, and it must be here
        self.check_replicas_instance(committed_instance,
                                     committed_instance_deps,
                                     [self.replicas[0]] + self.replicas[-self.fast_path_quorum_size:],
                                     state=(IS.EXECUTED, IS.COMMITTED))
        self.check_replicas_instance(failed_instance,
                                     {committed_instance, successful_instance},
                                     self.replicas[-self.fast_path_quorum_size:],
                                     state=(IS.EXECUTED, IS.COMMITTED))
        self.check_replicas_instance(successful_instance,
                                     {committed_instance, failed_instance},
                                     self.replicas[-self.fast_path_quorum_size:],
                                     state=(IS.EXECUTED, IS.COMMITTED))

        expected_order = [committed_instance]

        # with RF3, the failed instance was only recorded on a single node
        # so the prepare phase should have committed it as a noop
        if (len(self.replicas) - self.fast_path_quorum_size) == 1:
            for replica in self.replicas[-self.fast_path_quorum_size:]:
                self.assertTrue(replica.instances[failed_instance.iid].noop)
        else:
            # otherwise, we expect it to be executed
            expected_order.append(failed_instance)


        expected_order.append(successful_instance)
        self.check_execution_order(self.replicas[-self.fast_path_quorum_size:], expected_order)
        # self.check_execution_order(self.replicas, expected_order, allow_unexecuted=True)


class EpaxosIntegrationTestReplicaLeaderRF3(EpaxosIntegrationTest):
    replication_factor = 3
    leader_is_replica = True

@skip
class EpaxosIntegrationTestNonReplicaLeaderRF3(EpaxosIntegrationTest):
    replication_factor = 3
    leader_is_replica = False

class EpaxosIntegrationTestReplicaLeaderRF5(EpaxosIntegrationTest):
    replication_factor = 5
    leader_is_replica = True

@skip
class EpaxosIntegrationTestNonReplicaLeaderRF5(EpaxosIntegrationTest):
    replication_factor = 5
    leader_is_replica = False

class EpaxosIntegrationTestReplicaLeaderRF7(EpaxosIntegrationTest):
    replication_factor = 7
    leader_is_replica = True

@skip
class EpaxosIntegrationTestNonReplicaLeaderRF7(EpaxosIntegrationTest):
    replication_factor = 7
    leader_is_replica = False
