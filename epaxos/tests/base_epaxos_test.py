from unittest import TestCase

from epaxos.replica import Replica

class EpaxosTestCase(TestCase):

    def setUp(self):
        super(EpaxosTestCase, self).setUp()
        self.next_replica_name = 0
        self.replicas = []

    def create_replicas(self, num, replica_list=None):
        created_replicas = []
        for _ in range(num):
            created_replicas.append(Replica(self.next_replica_name))
            self.next_replica_name += 1

        replicas = self.replicas if replica_list is None else replica_list
        replicas.extend(created_replicas)

        # register new replicas
        for local in replicas:
            for remote in replicas:
                if remote is local:
                    continue
                local.add_peer(remote)

        return created_replicas
