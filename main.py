#!/usr/bin/env python

import argparse
import importlib
import logging
import sys
import unittest

from private import Environment
from public import ClientProcess, Process


def await(env, *futures, **kwargs):
    start_time = env.time
    time_limit = kwargs.pop("time_limit", 100)
    while not all(future.has_value for future in futures) and env.time - start_time < time_limit:
        env.step_randomly()
    if not any(future.has_value for future in futures):
        raise RuntimeError("some futures were not fulfilled within the time limit")


class BaseTestCase(unittest.TestCase):
    def __init__(self, impl_cls):
        super(BaseTestCase, self).__init__()
        self.impl_cls = impl_cls

    def setUp(self):
        super(BaseTestCase, self).setUp()
        if logging.root.isEnabledFor(logging.DEBUG):
            sys.stderr.write("\n")  # be nice with text test runner


class OneProcessSetGetTestCase(BaseTestCase):
    """check that the key-value protocol works correctly with only one process"""

    def runTest(self):
        env = Environment()
        client = env.spawn_process(ClientProcess)  # type: ClientProcess
        process = env.spawn_process(self.impl_cls)
        env.setup()

        result = client.call(process.pid, "set", key="the-key", value="the-value")
        env.step_by_ticking_process(client)
        env.step_by_delivering_messages(client)
        await(env, result)

        self.assertEqual(result.get_value()["value"], "the-value")
        self.assertEqual(result.get_value()["flag"], True)

        result = client.call(process.pid, "set", key="the-key", value="the-other-value")
        env.step_by_ticking_process(client)
        env.step_by_delivering_messages(client)
        await(env, result)

        self.assertEqual(result.get_value()["value"], "the-value")
        self.assertEqual(result.get_value()["flag"], False)

        result = client.call(process.pid, "get", key="the-key")
        env.step_by_ticking_process(client)
        env.step_by_delivering_messages(client)
        await(env, result)

        self.assertEqual(result.get_value()["value"], "the-value")


class ThreeProcessLearnSameValueTestCase(BaseTestCase):
    """check that all the processes learn the same value"""

    def runTest(self):
        env = Environment()
        client = env.spawn_process(ClientProcess)  # type: ClientProcess
        p1 = env.spawn_process(self.impl_cls)
        p2 = env.spawn_process(self.impl_cls)
        p3 = env.spawn_process(self.impl_cls)
        env.setup()

        result = client.call(p1.pid, "set", key="the-key", value="the-value")
        env.step_by_ticking_process(client)
        env.step_by_delivering_messages(client)
        await(env, result)

        self.assertEqual(result.get_value()["value"], "the-value")
        self.assertEqual(result.get_value()["flag"], True)

        results = [
            client.call(p1.pid, "get", key="the-key"),
            client.call(p2.pid, "get", key="the-key"),
            client.call(p3.pid, "get", key="the-key"),
        ]
        env.step_by_ticking_process(client)
        env.step_by_delivering_messages(client)
        await(env, *results)

        for result in results:
            self.assertEqual(result.get_value()["value"], "the-value")


class ThreeProcessConcurrentSetsTestCase(BaseTestCase):
    """check that the learned value was actually proposed"""

    def runTest(self):
        n = 3

        env = Environment()
        client = env.spawn_process(ClientProcess)  # type: ClientProcess
        processes = [env.spawn_process(self.impl_cls) for _ in range(n)]
        env.setup()

        proposals = ["the-value-%s" % i for i in range(n)]
        results = [
            client.call(process.pid, "set", key="the-key", value=proposal)
            for process, proposal in zip(processes, proposals)
        ]
        env.step_by_ticking_process(client)
        env.step_by_delivering_messages(client)
        await(env, *results)

        decided_value = None
        for result, proposal in zip(results, proposals):
            self.assertIn(result.get_value()["value"], proposals)
            if result.get_value()["flag"]:
                self.assertIsNone(decided_value)
                decided_value = result.get_value()["value"]
                self.assertEqual(decided_value, proposal)
        self.assertIsNotNone(decided_value)

        results = [
            client.call(process.pid, "get", key="the-key")
            for process in processes
        ]
        env.step_by_ticking_process(client)
        env.step_by_delivering_messages(client)
        await(env, *results)

        for result in results:
            self.assertEqual(result.get_value()["value"], decided_value)


def load_impl(path):
    parts = path.split(".")
    if len(parts) != 2:
        raise RuntimeError("implementation path must be in form MODULE.CLASS")
    name, klass = path.split(".")
    module = importlib.import_module(name)
    if klass not in dir(module):
        raise RuntimeError("module %s is missing class %s" % (module, klass))
    cls = module.__dict__[klass]
    if not issubclass(cls, Process):
        raise RuntimeError("class %s must be derived from Process" % klass)
    return cls


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--impl", metavar="MODULE.CLASS", required=True,
                        help="implementation to test")
    parser.add_argument("-l", "--list", action="store_true",
                        help="list all tests and exit")
    parser.add_argument("-g", "--grep", metavar="SUBSTRING",
                        help="run only tests with given substring in its name")
    parser.add_argument("-r", "--repeat", metavar="N", type=int, default=1,
                        help="repeat all the tests given number of times")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="be verbose")
    args = parser.parse_args()

    impl_cls = load_impl(args.impl)

    tests = [
        OneProcessSetGetTestCase(impl_cls),
        ThreeProcessLearnSameValueTestCase(impl_cls),
        ThreeProcessConcurrentSetsTestCase(impl_cls),
    ]

    if args.grep:
        tests = filter(lambda t: args.grep.lower() in str(t).lower(), tests)

    if args.list:
        for test in tests:
            print(str(test))
        return

    if args.verbose:
        logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.DEBUG)
    else:
        logging.disable(logging.DEBUG)

    runner = unittest.TextTestRunner(verbosity=(2 if args.verbose else 1))

    iteration = 0
    while iteration < args.repeat:
        logging.debug("*" * 80)
        logging.debug("*" * 10 + " ITERATION %-8d " + "*" * 50, iteration + 1)
        logging.debug("*" * 80)
        suite = unittest.TestSuite()
        suite.addTests(tests)
        result = runner.run(suite)
        if not result.wasSuccessful():
            return 42
        iteration += 1


if __name__ == "__main__":
    sys.exit(main())
