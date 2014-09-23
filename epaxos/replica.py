from collections import defaultdict
from collections import namedtuple
from copy import deepcopy, copy
import random
from uuid import uuid1, UUID


from tarjan import tarjan

class BallotFailure(Exception): pass
class InvalidPromotionException(Exception): pass
class QuorumFailure(Exception): pass
class InstanceDoesNotExist(Exception): pass
class NetworkFailure(Exception): pass
class UnknownError(Exception): pass
class CantPrepare(Exception): pass
class Stuck(Exception): pass


class Instance(object):

    class State(object):
        PREACCEPTED = 1
        ACCEPTED = 2
        COMMITTED = 3
        EXECUTED = 4

        @classmethod
        def to_string(cls, val):
            return {
                cls.PREACCEPTED: "PREACCEPTED",
                cls.ACCEPTED: "ACCEPTED",
                cls.COMMITTED: "COMMITTED",
                cls.EXECUTED: "EXECUTED"
            }[val]

    def __init__(self, leader, replicas, nickname=None):
        assert isinstance(leader, Replica)
        assert isinstance(replicas, set)

        self.leader = leader.name
        self.replicas = replicas
        self.leader_is_replica = self.leader in self.replicas
        self.nickname = nickname

        self.iid = uuid1()
        self.state = 0
        self.ballot = 0
        self.noop = False

        self.dependencies = None

        # indicates that, on preaccept, a
        # non-command leader agreed with
        # the dependencies attached to the
        # preaccept request form the command leader
        self.leader_deps_match = False

        # set by a replica command leader if
        # a command could not be committed on
        # the fast path for any reasonse
        self.fast_path_impossible = False

        self.preaccept_deps = None
        self.accept_deps = None
        self.commit_deps = None
        self.trypreaccept_deps = None

    def __repr__(self):
        parts = ["Instance"]
        if self.nickname: parts.append("'{}'".format(self.nickname))
        parts.append("[{} {}]".format(self.State.to_string(self.state), self.iid))
        return ' '.join(parts)

    @property
    def N(self):
        return len(self.replicas)

    @property
    def F(self):
        return self.N / 2

    @property
    def fast_path_quorum_size(self):
        return self.F + ((self.F + 1) / 2)

    @property
    def quorum_size(self):
        return self.F + 1

    def reset_for_local(self):
        """
        removes deepcopied attributes from remote node
        :return:
        """
        # self.dependencies = None
        self.leader_deps_match = False
        self.strongly_connected = None
        self.event_log = []
        return self

    def preaccept(self, deps, leader_instance=None, trypreaccept=False):
        if self.state > Instance.State.PREACCEPTED:
            raise InvalidPromotionException()
        if deps is not None:
            self.dependencies = set(deps) - {self.iid}
        self.state = Instance.State.PREACCEPTED
        if leader_instance is not None:
            assert isinstance(leader_instance, Instance)
            self.leader_deps_match = self.dependencies == leader_instance.dependencies

        if trypreaccept:
            self.trypreaccept_deps = (self.dependencies or set()).copy()
        else:
            self.preaccept_deps = (self.dependencies or set()).copy()

    def accept(self, deps, ballot):
        assert ballot >= self.ballot
        if self.state >= Instance.State.ACCEPTED:
            raise InvalidPromotionException()
        self.dependencies = set(deps) - {self.iid}
        self.ballot = ballot
        self.state = Instance.State.ACCEPTED
        self.accept_deps = (self.dependencies or set()).copy()

    def commit(self, deps=None, ballot=None):
        if self.state > Instance.State.COMMITTED:
            raise InvalidPromotionException()
        if deps is not None:
            self.dependencies = set(deps) - {self.iid}
            if ballot is not None:
                self.ballot = max(self.ballot, ballot)
        self.state = Instance.State.COMMITTED
        self.commit_deps = (self.dependencies or set()).copy()

    def mark_executed(self):
        if self.state != Instance.State.COMMITTED:
            raise InvalidPromotionException()
        self.state = Instance.State.EXECUTED

    def incr_ballot(self):
        self.ballot += 1

    def update_ballot(self, ballot):
        self.ballot = max(ballot, self.ballot)

# Messages
TimeoutResponse = "TimeoutResponse"
Executed = "Executed"
FastPathImpossible = "FastPathImpossible"
PackedResponse = namedtuple("PackedResponse", ("response", "replica"))
PreacceptRequest = namedtuple("PreacceptRequest", ("instance",))
PreacceptResponse = namedtuple("PreacceptResponse", ("instance", "missing_instances", "problem"))
AcceptRequest = namedtuple("AcceptRequest", ("instance", "missing_instances"))
AcceptResponse = namedtuple("AcceptResponse", ("rejected_ballot",))
CommitRequest = namedtuple("CommitRequest", ("instance",))
PrepareRequest = namedtuple("PrepareRequest", ("iid", "ballot"))
PrepareResponse = namedtuple("PrepareResponse", ("instance", "problem"))
TryPreacceptRequest = namedtuple("TryPreacceptRequest", ("instance", "dependencies"))
TryPreacceptResponse = namedtuple("TryPreacceptResponse", ("success",))
TryPreacceptTuple = namedtuple("TryPreacceptTuple", ("deps", "replicas", "num_agreeing"))
InstanceRequest = namedtuple("InstanceRequest", ("iid",))
InstanceResponse = namedtuple("InstanceResponse", ("instance",))

# message send/receive decorators
def send(func):
    def wrapper(self, instance, *args, **kwargs):
        if self.state == Replica.State.DOWN:
            raise NetworkFailure()
        return func(self, instance, *args, **kwargs)
    return wrapper

def receive(func):
    def wrapper(self, instance, *args, **kwargs):
        if self.state >= Replica.State.NORESPONSE:
            if self.state == Replica.State.NORESPONSE:
                try:
                    # process the request, but don't respond
                    func(self, instance, *args, **kwargs)
                except:
                    pass
            return TimeoutResponse
        return func(self, instance, *args, **kwargs)
    return wrapper

class Replica(object):

    class State(object):
        UP = 1  # node is sending and receiving messages normally
        NORESPONSE = 2  # node is receiving messages, but responses time out
        DOWN = 3 # node doesn't receive messages

        @classmethod
        def to_string(cls, val):
            return {
                cls.UP: "UP",
                cls.NORESPONSE: "NORESPONSE",
                cls.DOWN: "DOWN"
            }[val]

    def __init__(self, name):
        self.name = name
        self.peers = {}
        self.state = self.State.UP
        self.instances = {}
        self.executed = []
        self.last_created_instance = None

    def __repr__(self):
        return "Replica {} {}".format(self.name, self.State.to_string(self.state))

    def add_peer(self, peer):
        assert isinstance(peer, Replica)
        self.peers[peer.name] = peer

    def current_deps(self):
        return set(self.instances.keys())

    def check_remote_ballot(self, remote_instance):
        assert isinstance(remote_instance, Instance)
        if remote_instance.iid in self.instances:
            instance = self.instances[remote_instance.iid]
            if instance.ballot >= remote_instance.ballot:
                return instance.ballot

    def check_quorum_response(self, instance, responses):
        if len(responses) + (1 if self.name in instance.replicas else 0) < instance.quorum_size:
            raise QuorumFailure()

    def request_instance(self, iid):
        msg = InstanceRequest(iid)
        responses = [peer.handle_instance_request(msg) for peer in self.peers.values()]
        responses = filter(lambda m: m != TimeoutResponse, responses)

        # get the remote ballot with the highest ballot
        instances = [r.instance for r in responses if r.instance is not None]
        instance = sorted(instances, key=lambda x: -x.ballot)[0] if instances else None

        if not instances:
            return

        instance = deepcopy(instance).reset_for_local()
        if instance.state > Instance.State.COMMITTED:
            instance.state = Instance.State.COMMITTED
        elif instance.state < Instance.State.ACCEPTED:
            # if the instance is preaccepted, we can't trust it's dependencies
            # without affecting the outcome of prepare
            instance.dependencies = None

        self.instances.setdefault(instance.iid, instance)
        return instance

    @receive
    def handle_instance_request(self, message):
        assert isinstance(message, InstanceRequest)
        instance = self.instances.get(message.iid)
        if instance is not None and self.name not in instance.replicas:
            instance = None
        return InstanceResponse(instance)

    def get_instance(self, replicas, nickname=None):
        """ takes either replica names or instances """
        replicas = {r.name if isinstance(r, Replica) else r for r in replicas}
        return Instance(self, replicas, nickname=nickname)

    def coordinate_request(self, replicas, preaccept_hook=None, accept_hook=None, commit_hook=None, nickname=None):
        """ coordinate a new instance from this replica """
        instance = self.get_instance(replicas, nickname=nickname)
        self.last_created_instance = instance

        if preaccept_hook is not None:
            preaccept_hook()

        preaccept_responses = self.preaccept(instance)

        if accept_hook is not None:
            accept_hook()

        self.maybe_accept(instance, preaccept_responses)

        if commit_hook is not None:
            commit_hook()

        instance.commit()
        if instance.leader_is_replica:
            self.send_commit(instance)
            result = self.execute(instance)
            # clean up the other replicas
            for peer in [self.peers[r] for r in instance.replicas if r != self.name]:
                if peer.state == Replica.State.UP:
                    peer.execute(peer.instances.get(instance.iid))
            return result
        else:
            result = None
            self.send_commit(instance)
            # clean up the other replicas
            for peer in [self.peers[r] for r in instance.replicas]:
                if peer.state != Replica.State.UP:
                    continue
                result = peer.execute(peer.instances.get(instance.iid))

            return result

    def preaccept(self, instance):
        # only calculate dependencies and persist instance
        # locally if this is a replica of the instance
        if self.name in instance.replicas:
            instance.preaccept(self.current_deps())
            self.instances[instance.iid] = instance
        else:
            instance.preaccept(None)

        return self.send_preaccept(instance)

    @send
    def send_preaccept(self, instance):
        instance.incr_ballot()
        responses = [PackedResponse(self.peers[r].handle_preaccept(PreacceptRequest(deepcopy(instance))), r)
                     for r in instance.replicas if r != self.name]

        # filter out timeout responses
        responses = filter(lambda m: m.response != TimeoutResponse, responses)

        if self.name in instance.replicas:
            for response in responses:
                for missing in response.response.missing_instances:
                    missing = deepcopy(missing).reset_for_local()
                    if missing.state < Instance.State.ACCEPTED:
                        missing.dependencies = None
                    self.instances.setdefault(missing.iid, missing)

        # check we didn't get anything weird back
        assert all([isinstance(r.response, PreacceptResponse) for r in responses])

        # add any missing instances the replica is sending back

        errors = {r.response.problem for r in responses} - {None}
        if BallotFailure in errors:
            instance.update_ballot(max([r.response.instance.ballot for r in responses]))
            # if we received a ballot failure, it means that another replica is attempting
            # to prepare this instance. Bail out and let it finish
            raise BallotFailure()

        if len(errors) != 0:
            raise UnknownError("Unhandled errors returned: {}".format(errors))

        try:
            self.check_quorum_response(instance, responses)
        except QuorumFailure:
            instance.fast_path_impossible = True
            raise

        return responses

    @receive
    def handle_preaccept(self, message):
        """
        if the remote ballot checks out, run a preaccept phase. In any case, if we have instances that
        this instance depends on that the remote node doesn't know about, return them in the response
        """
        assert isinstance(message, PreacceptRequest)
        msg_instance = message.instance
        assert isinstance(msg_instance, Instance)

        run_preaccept = not self.check_remote_ballot(msg_instance)
        instance = self.instances.get(msg_instance.iid, deepcopy(msg_instance).reset_for_local())
        instance.dependencies = None
        if run_preaccept:
            instance.preaccept(self.current_deps(), msg_instance)
        missing_instances = [self.instances[iid] for iid in (instance.dependencies - (msg_instance.dependencies or set()))]
        self.instances[instance.iid] = instance
        return PreacceptResponse(deepcopy(instance), missing_instances, None if run_preaccept else BallotFailure)

    def maybe_accept(self, instance, responses):
        """
        If the instances returned by the preaccept responses either don't match our deps, or
        there weren't enough responses to satisfy a fast path quorum, run an accept phase
        """

        dep_groups = set()
        deps = set()
        response_instances = [r.response.instance for r in responses] + ([instance] if self.name in instance.replicas else [])
        for inst in response_instances:
            deps.update(inst.dependencies)
            dep_groups.add(frozenset(inst.dependencies))

        # if none of the replicas had a different view of interfering dependencies, and we
        # received enough responses to satisfy a fast-path quorum, we can skip the accept phase
        if len(dep_groups) == 1 and len(response_instances) >= instance.fast_path_quorum_size:
            return

        instance.fast_path_impossible = True

        missing_instances = {}
        if self.name in inst.dependencies:
            for response in responses:
                response, replica = response
                missing_instances[replica] = [deepcopy(self.instances.get(i)) for i in (deps - response.instance.dependencies)]

        self.accept(instance, deps, max([instance.ballot] + [i.ballot for i in response_instances]), missing_instances=missing_instances)

    def accept(self, instance, deps, ballot, missing_instances=None):
        assert deps != None
        instance.accept(deps, ballot)
        self.send_accept(instance, missing_instances=missing_instances)

    @send
    def send_accept(self, instance, missing_instances=None):
        missing_instances = missing_instances or {}
        instance.incr_ballot()

        responses = [self.peers[r].handle_accept(AcceptRequest(deepcopy(instance), missing_instances.get(r, []))) for r in instance.replicas if r != self.name]

        # filter out timeout responses
        responses = filter(lambda m: m != TimeoutResponse, responses)

        # check we didn't get anything weird back
        assert all([isinstance(r, AcceptResponse) for r in responses])

        ballot_failures = {r.rejected_ballot for r in responses if r.rejected_ballot is not None}
        if ballot_failures:
            instance.update_ballot(max(ballot_failures))
            # if we received a ballot failure, it means that another replica is attempting
            # to prepare this instance. Bail out and let it finish
            raise BallotFailure()

        # if we didn't receive a response from a quorum of nodes, we can't continue
        self.check_quorum_response(instance, responses)

    @receive
    def handle_accept(self, message):
        """
        accept the remote instances deps & seq
        """
        assert isinstance(message, AcceptRequest)
        msg_instance = message.instance
        assert isinstance(msg_instance, Instance)

        for missing in message.missing_instances:
            missing = deepcopy(missing).reset_for_local()
            if missing.state < Instance.State.ACCEPTED:
                missing.dependencies = None
            self.instances.setdefault(missing.iid, missing)

        instance = self.instances.get(msg_instance.iid)

        if instance is not None and self.check_remote_ballot(msg_instance):
            return AcceptResponse(self.check_remote_ballot(msg_instance))

        instance = instance or deepcopy(msg_instance).reset_for_local()

        if instance.state < Instance.State.ACCEPTED:
            instance.accept(msg_instance.dependencies, msg_instance.ballot)

        assert instance.dependencies is not None
        self.instances[instance.iid] = instance
        return AcceptResponse(None)

    @send
    def send_commit(self, instance):
        instance.incr_ballot()
        responses = []
        for replica in [self.peers[r] for r in instance.replicas if r != self.name]:
            try:
                responses.append(replica.handle_commit(CommitRequest(deepcopy(instance))))
            except (QuorumFailure, BallotFailure):
                # replicas will be able to figure out what happened if they can't
                # receive the commit right now, so swallow any exceptions
                pass

        responses = filter(lambda m: m != TimeoutResponse, responses)
        return responses[0] if responses else None

    @receive
    def handle_commit(self, message):
        """
        mark the message as committed, don't check ballots for this message
        :param message:
        :return:
        """
        assert isinstance(message, CommitRequest)
        msg_instance = message.instance
        assert isinstance(msg_instance, Instance)

        instance = self.instances.get(msg_instance.iid)

        # make sure nothing stupid has happened
        skip_commit = False
        if instance is not None:
            if instance.state >= Instance.State.COMMITTED:
                assert instance.dependencies == msg_instance.dependencies
                skip_commit = True
        else:
            instance = deepcopy(msg_instance).reset_for_local()
        self.instances[instance.iid] = instance

        if not skip_commit:
            instance.commit(msg_instance.dependencies, msg_instance.ballot)

        assert instance.dependencies is not None
        # return self.execute(instance)

    def execute(self, instance):
        assert isinstance(instance, Instance)
        assert self.name in instance.replicas

        # build a directed graph of dependencies
        dep_graph = {}
        uncommitted = set()
        def build_graph(inst):
            cant_execute = True
            assert isinstance(inst, Instance)
            if inst.iid in dep_graph:
                return
            if inst.state < Instance.State.COMMITTED:
                uncommitted.add(inst.iid)

            if inst.dependencies is None:
                assert inst.state < Instance.State.ACCEPTED
                uncommitted.add(inst.iid)
                return True
            dep_graph[inst.iid] = inst.dependencies.copy()
            for iid in inst.dependencies:
                if iid not in self.instances:
                    dep_inst = deepcopy(self.request_instance(iid)).reset_for_local()
                    if dep_inst.state < Instance.State.ACCEPTED:
                        uncommitted.add(iid)
                        continue

                assert iid in self.instances, self

                if iid not in self.instances:
                    cant_execute = False
                    continue

                cant_execute = build_graph(self.instances[iid]) and cant_execute

            return cant_execute

        # strongly connected components are arbitrarily ordered
        # by their uuid1 ids
        def scc_comparator(x, y):
            x = self.instances[x]
            y = self.instances[y]
            xID = x.iid if isinstance(x.iid, UUID) else UUID(x.iid)
            yID = y.iid if isinstance(x.iid, UUID) else UUID(y.iid)
            if xID.time != yID.time:
                return int(xID.time - yID.time)
            else:
                return -1 if xID.bytes < yID.bytes else 1

        def get_execution_order():
            dep_graph.clear()
            uncommitted.clear()
            can_execute = build_graph(instance)
            sc_components = tarjan(dep_graph)
            return can_execute, sum([sorted(c, cmp=scc_comparator) for c in sc_components], [])

        # preparing instances may change the calculated execution order
        while True:
            # a check that a committed instances wasn't preceded
            # by an uncommitted one for some terrible reason
            last_instance_uncommitted = False

            can_execute, execution_order = get_execution_order()

            if uncommitted:
                for iid in uncommitted:
                    try:
                        self.prepare(self.instances[iid])
                    except CantPrepare:
                        if len(uncommitted) < 2:
                            raise Stuck
                continue

            # execute the uncommitted instances
            for iid in execution_order:
                to_execute = self.instances[iid]
                assert isinstance(to_execute, Instance)

                assert to_execute.state >= Instance.State.COMMITTED

                if to_execute.state == Instance.State.EXECUTED:
                    if to_execute.iid == instance.iid:
                        return Executed
                    continue

                to_execute.mark_executed()
                if not to_execute.noop:
                    self.executed.append(iid)

                if iid == instance.iid:
                    return Executed

    def prepare(self, instance):
        responses = self.send_prepare(instance)

        if instance.state >= Instance.State.COMMITTED:
            return

        # if we can't get a quorum from the other replicas, there's no use continuing
        self.check_quorum_response(instance, responses)

        # check for committed responses
        committed = [r.response.instance for r in responses if r.response.instance is not None and r.response.instance.state >= Instance.State.COMMITTED]
        committed = committed[0] if committed else None
        if committed:
            assert committed.dependencies is not None
            instance.commit(committed.dependencies)
            self.send_commit(instance)
            return

        # include the local instance in the responses to inspect.
        # since prepare is only attempted at execution time, and execution is only performed
        # by replicas, we don't need to check if we're a replica
        this_response = [PackedResponse(PrepareResponse(instance, None), self.name)] if instance.dependencies is not None else []
        augmented_responses = responses + this_response

        # check for accepted responses, including ourself
        # if any replicas accepted this instance, it means that it's been recorded by them, and will
        # be a dependency of subsequent interfering instances
        accepted = [r.response.instance for r in augmented_responses if r.response.instance is not None and r.response.instance.state == Instance.State.ACCEPTED]
        accepted = sorted(accepted, key=lambda x: -x.response.ballot)[0] if accepted else None
        if accepted:
            assert accepted.dependencies is not None
            self.accept(instance, accepted.dependencies, accepted.ballot)
            instance.commit()
            self.send_commit(instance)
            return

        # this is where optimized epaxos gets complicated
        # see section 4.4 of http://sigops.org/sosp/sosp13/papers/p358-moraru.pdf
        if not self.trypreaccept_phase(instance, augmented_responses):
            # commit a noop if no other instances have seen it
            instance.noop = len([r for r in responses if r.response.instance is not None]) == 0
            self.preaccept(instance)
            self.accept(instance, instance.dependencies, instance.ballot)
            instance.commit()
            assert instance.dependencies is not None
            self.send_commit(instance)

    @send
    def send_prepare(self, instance):
        while True:
            instance.incr_ballot()
            # record the peer from each response
            # TODO: do the same for the preaccept responses, so accept messages can include missing instances
            responses = [PackedResponse(self.peers[r].handle_prepare(PrepareRequest(instance.iid, instance.ballot)), r) for r in instance.replicas if r != self.name]

            # filter out timeout responses
            responses = filter(lambda m: m.response != TimeoutResponse, responses)

            # check we didn't get anything weird back
            assert all([isinstance(r.response, PrepareResponse) for r in responses])

            errors = {r.response.problem for r in responses} - {None}
            if BallotFailure in errors:
                instance.update_ballot(max([r.response.instance.ballot for r in responses if r.response.instance is not None]))
                # if we received a ballot failure, keep incrementing the ballot until we succeed
                # in the real world, this would be a great way to get stuck in live lock
                continue

            if len(errors) != 0:
                raise UnknownError("Unhandled errors returned: {}".format(errors))

            self.check_quorum_response(instance, responses)

            return responses

    @receive
    def handle_prepare(self, message):
        assert isinstance(message, PrepareRequest)
        instance = self.instances.get(message.iid)
        if instance is None:
            return PrepareResponse(None, None)
        return PrepareResponse(deepcopy(instance), BallotFailure if instance.ballot >= message.ballot else None)

    def _get_trypreaccept_deps_and_replicas(self, instance, responses):
        # min number of identical preaccepts
        mnip = int((instance.F + 1) / 2)

        dep_groups = defaultdict(set)
        dep_scores = defaultdict(lambda: 0)
        relevant_replicas = set()
        for response in responses:
            inst = response.response.instance
            deps = frozenset(inst.dependencies) if inst is not None and inst.dependencies is not None else None
            dep_groups[deps].add(response.replica)
            relevant_replicas.add(response.replica)

            # the deps that agreed with the leader should always be tried first
            score = 1 + (int(inst.leader_deps_match) * len(instance.replicas)) - int(inst.fast_path_impossible)
            dep_scores[deps] = score


        attempts = []
        for deps, replicas in dep_groups.items():
            if deps is None:
                continue
            if len(replicas) < mnip:
                continue
            recipients = relevant_replicas - set(replicas)
            attempts.append(TryPreacceptTuple(deps, recipients, len(replicas)))

        # make sure the ordering of the attempts doesn't obscure any problems
        random.shuffle(attempts)

        # order the attempts so that deps that matched the command leader's deps are
        # attempted first, and the failed command leader's deps are tried last
        attempts.sort(key=lambda a: -dep_scores[a.deps])

        return attempts

    def trypreaccept_phase(self, instance, responses):
        assert isinstance(instance, Instance)
        assert all([r.response.instance is None or r.response.instance.state == Instance.State.PREACCEPTED for r in responses])

        instances = filter(None, [r.response.instance for r in responses])
        fast_path_impossible = any([i.fast_path_impossible for i in instances])
        if fast_path_impossible:
            return False

        for deps, replicas, agreeing_replicas in self._get_trypreaccept_deps_and_replicas(instance, responses):
            assert deps is not None
            responses = self.send_trypreaccept(instance, deps, replicas)
            required_successes = (instance.F + 1) - agreeing_replicas
            if len([r for r in responses if r.response.success]) >= required_successes:
                self.accept(instance, deps, instance.ballot)
                instance.commit()
                assert instance.dependencies is not None
                self.send_commit(instance)
                return True

        return False

    @send
    def send_trypreaccept(self, instance, deps, replicas):
        target_instance = deepcopy(instance)
        target_instance.dependencies = deps
        message = TryPreacceptRequest(instance, deps)
        responses = []
        for r in replicas:
            if r == self.name:
                responses.append(PackedResponse(self.handle_trypreaccept(message), r))
            else:
                responses.append(PackedResponse(self.peers[r].handle_trypreaccept(message), r))
        return responses

    @receive
    def handle_trypreaccept(self, message):
        assert isinstance(message, TryPreacceptRequest)
        msg_instance = deepcopy(message.instance).reset_for_local()
        msg_instance.dependencies = message.dependencies
        assert isinstance(msg_instance, Instance)

        # get instances that the message instance doesn't have in it's dependencies
        possible_conflicts = set(self.instances.keys()) - set(message.dependencies) - {msg_instance.iid}

        # if this replica hasn't seen the some of the proposed dependencies, don't preaccept it
        valid_instance = lambda i: i.state > Instance.State.PREACCEPTED or i.dependencies is not None
        if not all([iid in self.instances and valid_instance(self.instances[iid]) for iid in msg_instance.dependencies]):
            return TryPreacceptResponse(False)

        for conflict in [self.instances[iid] for iid in possible_conflicts]:
            assert isinstance(conflict, Instance)

            if conflict.iid == msg_instance.iid:
                continue

            # FIXME: requiring the potential conflict be committed causes an infinite loop of prepares
            # the potential conflict needs to be committed before
            # we can determine if it's a real conflict
            if conflict.state < Instance.State.COMMITTED:
                raise CantPrepare

                # self.prepare(conflict)

            # if the proposed message isn't in the possible conflicts
            # dependencies, it couldn't have been committed on the
            # fast path
            if msg_instance.iid not in conflict.dependencies:
                return TryPreacceptResponse(False)

        local_instance = self.instances.setdefault(msg_instance.iid, deepcopy(msg_instance).reset_for_local())
        assert isinstance(local_instance, Instance)
        assert local_instance.state <= Instance.State.PREACCEPTED

        local_instance.preaccept(message.dependencies)
        assert local_instance.dependencies is not None
        return TryPreacceptResponse(True)




