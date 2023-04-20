from io import StringIO
import subprocess
import datetime
import os
import argparse
import tempfile
from colorama import Fore

CWD = "./src/raft"
LOG_PATH = "./logs"
TIME_FORMAT = "%Y-%m-%dT%H-%M-%S"
MAX_FAILURE_TIME_SINGLE = 1

TestSuites = {
    "2A": ["TestInitialElection2A", "TestReElection2A", "TestManyElections2A"],
    "2B": ["TestBasicAgree2B", "TestRPCBytes2B", "TestFollowerFailure2B", "TestLeaderFailure2B", "TestFailAgree2B", "TestFailNoAgree2B", "TestConcurrentStarts2B", "TestRejoin2B", "TestBackup2B", "TestCount2B"],
    "2C": ["TestPersist12C", "TestPersist22C", "TestPersist32C", "TestFigure82C", "TestUnreliableAgree2C", "TestFigure8Unreliable2C", "TestReliableChurn2C", "TestUnreliableChurn2C"],
    "2D": ["TestSnapshotBasic2D", "TestSnapshotInstall2D", "TestSnapshotInstallUnreliable2D", "TestSnapshotInstallCrash2D", "TestSnapshotInstallUnCrash2D", "TestSnapshotAllCrash2D", "TestSnapshotInit2D"],
}


class Color:
    @staticmethod
    def red(s: str):
        return Fore.RED + s + Fore.RESET


class SingleTestCaseRecord:
    def __init__(self, case: str) -> None:
        self.testCaseName = case
        self.cases = []
        self.failures = 0

    def add(self, timecost, ret):
        if not ret:
            self.failures += 1
        else:
            self.cases.append(timecost)

    def avg(self):
        if len(self.cases) == 0:
            return 0
        sum = 0
        for i in self.cases:
            sum += int(i.seconds)
        return sum / len(self.cases)

    def category(self) -> str:
        return self.testCaseName[len(self.testCaseName)-2:].upper()


class CategoryRecord:
    def __init__(self, category) -> None:
        self.category = category
        self.tests = []

    def add(self, test):
        self.tests.append(test)

    def print(self):
        avg = 0
        failures = ""
        logs = []

        for record in self.tests:
            avg += record.avg()
            if record.failures > 0:
                failures += record.testCaseName + ","
                logs.append(Color.red("[{}] avg={}s, failure={}({})".format(
                    record.testCaseName, record.avg(), record.failures, record.failures + len(record.cases), )))
            else:
                logs.append("[{}] avg={}s".format(
                    record.testCaseName, record.avg()))

        summary = "========= {}, avg={}".format(self.category, avg)
        if len(failures) > 0:
            summary = Color.red("{}, failures={}".format(summary, failures))

        print(summary)
        for log in logs:
            print(log)


class Logger:

    def __init__(self) -> None:
        self.records = {}

    def runOneTest(self, fn):
        def __wrapper(testCase: str):
            start = datetime.datetime.now()
            ret = fn(testCase)
            dur = datetime.datetime.now() - start

            record = self.records.get(testCase, SingleTestCaseRecord(testCase))
            record.add(dur, ret)
            self.records[testCase] = record
            return ret
        return __wrapper

    def countByCategory(self):
        categoryRecords = {}
        for record in self.records.values():
            category = record.category()
            categoryRecord = categoryRecords.get(
                category, CategoryRecord(category))
            categoryRecord.add(record)
            categoryRecords[category] = categoryRecord

        print("test finish running, reports: ")
        for c in categoryRecords.values():
            c.print()


logger = Logger()


@logger.runOneTest
def runTest(testCase: str):
    command = "go test -run {} -timeout 120s".format(testCase)

    out_temp = tempfile.TemporaryFile(mode='w+')
    fileno = out_temp.fileno()

    process = subprocess.Popen(
        command, shell=True, cwd=CWD, stdout=fileno, stderr=fileno)

    process.wait()

    if process.returncode != 0:
        folder = "{}/{}".format(LOG_PATH, testCase)
        os.makedirs(folder, exist_ok=True)

        file = "{}/{}".format(folder,
                              datetime.datetime.now().strftime(TIME_FORMAT))

        with open(file, "w+", encoding="utf-8") as f:
            out_temp.seek(0)
            f.write(out_temp.read())
        return False

    out_temp.close()

    return True


class FixedRoundRunner:
    def __init__(self, testCases: list[str], n: int) -> None:
        self.testCases = testCases
        self.n = n
        self.totalFailure = 0

    def run(self):
        for t in self.testCases:
            failures = 0
            for i in range(self.n):
                ret = runTest(t)
                if not ret:
                    failures += 1
                    self.totalFailure += 1
                    if failures >= MAX_FAILURE_TIME_SINGLE:
                        print("abort, failues greater than MAX_FAILURE_TIME_SINGLE")
                        break


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--running_round", '-r', default=1, type=int,
                        help="how many times will each testcase be run")
    parser.add_argument("--testcases", '-t', nargs='+',
                        help="testcases want to run")
    return parser.parse_args()


def parse_testcases(cases: list[str]) -> list[str]:
    if cases is None or len(cases) == 0:
        raise("no testcase to run")

    all = []
    for suite in TestSuites.values():
        all.extend([x for x in suite])

    testCases = []
    notFound = []
    for c in cases:
        if len(c) == 2 or 'a' <= c[1] <= 'd' or 'A' <= c[1] <= 'D':
            c = c.upper()
            testCases.extend(TestSuites[c])
        elif c in all:
            testCases.append(c)
        else:
            notFound.append(c)

    if len(notFound) > 0:
        print("warning, following testcases not found: {}".format(notFound))

    return list(set(testCases))


def main():
    args = parse_args()
    testCases = parse_testcases(args.testcases)

    if testCases is None or len(testCases) == 0:
        raise("no testcase to run")

    print("tests to be run: {}".format(testCases))

    runner = FixedRoundRunner(testCases, args.running_round)
    runner.run()

    logger.countByCategory()

    if runner.totalFailure > 0:
        os._exit(1)


if __name__ == "__main__":
    main()
