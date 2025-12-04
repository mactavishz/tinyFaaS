#!/usr/bin/env python3

import unittest

import json
import os
import os.path as path
import subprocess
import sys
import time
import typing
import urllib.error
import urllib.request

connection: typing.Dict[str, typing.Union[str, int]] = {
    "host": "localhost",
    "management_port": 8080,
    "http_port": 8000,
}

src_path = "."
fn_path = path.join(src_path, "test", "fns")
script_path = path.join(src_path, "scripts")

# Timeout for waiting for services to start (in seconds)
SERVICE_STARTUP_TIMEOUT = 30


def wait_for_service(host: str, port: int, timeout: int = SERVICE_STARTUP_TIMEOUT) -> bool:
    """Wait for a service to become available. Returns True if successful."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            urllib.request.urlopen(f"http://{host}:{port}/", timeout=2)
            return True
        except urllib.error.HTTPError:
            # HTTP error means service is up but returned an error (e.g., 404)
            return True
        except Exception:
            time.sleep(0.5)
            continue
    return False


def setUpModule() -> None:
    """Wait for tinyFaaS services to be ready"""
    host = connection["host"]
    
    print("Waiting for management service...")
    if not wait_for_service(host, connection["management_port"]):  # type: ignore
        raise RuntimeError(
            f"Management service at {host}:{connection['management_port']} "
            f"did not start within {SERVICE_STARTUP_TIMEOUT} seconds"
        )
    print("Management service is ready")
    
    print("Waiting for HTTP service...")
    if not wait_for_service(host, connection["http_port"]):  # type: ignore
        raise RuntimeError(
            f"HTTP service at {host}:{connection['http_port']} "
            f"did not start within {SERVICE_STARTUP_TIMEOUT} seconds"
        )
    print("HTTP service is ready")


def tearDownModule() -> None:
    """clean up after tests"""

    # call wipe-functions.sh
    try:
        subprocess.run(
            ["./wipe-functions.sh"], cwd=script_path, check=True, capture_output=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to wipe functions:\n{e.stderr.decode('utf-8')}")

    # call make clean
    try:
        subprocess.run(["make", "clean"], cwd=src_path, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to clean up:\n{e.stderr.decode('utf-8')}")

    return


def startFunction(folder_name: str, fn_name: str, env: str, threads: int) -> str:
    """starts a function, returns name"""

    # get full path of folder
    folder_name = os.path.abspath(folder_name)

    # use the upload.sh script
    try:
        result = subprocess.run(
            ["./upload.sh", folder_name, fn_name, env, str(threads)],
            cwd=script_path,
            check=True,
            capture_output=True,
        )
        print(f"Upload {fn_name}: {result.stdout.decode('utf-8')}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload function {fn_name}:")
        print(f"  stdout: {e.stdout.decode('utf-8')}")
        print(f"  stderr: {e.stderr.decode('utf-8')}")
        raise e

    # Wait for function container to be ready
    time.sleep(2)

    return fn_name


class TinyFaaSTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super(TinyFaaSTest, cls).setUpClass()

    def setUp(self) -> None:
        global connection
        self.host = connection["host"]
        self.http_port = connection["http_port"]


class TestSieve(TinyFaaSTest):
    fn = ""

    @classmethod
    def setUpClass(cls) -> None:
        cls.fn = startFunction(
            path.join(fn_path, "sieve-of-eratosthenes"), "sieve", "nodejs", 1
        )

    def setUp(self) -> None:
        super(TestSieve, self).setUp()
        self.fn = TestSieve.fn

    def test_invoke_http(self) -> None:
        """invoke a function"""

        # make a request to the function
        res = urllib.request.urlopen(
            f"http://{self.host}:{self.http_port}/{self.fn}", timeout=10
        )

        # check the response
        self.assertEqual(res.status, 200)

        return

    def test_invoke_http_async(self) -> None:
        """invoke a function async"""

        # make an async request to the function
        req = urllib.request.Request(
            f"http://{self.host}:{self.http_port}/{self.fn}",
            headers={"X-tinyFaaS-Async": "true"},
        )

        res = urllib.request.urlopen(req, timeout=10)

        # check the response
        self.assertEqual(res.status, 202)

        return
class TestEchoPY(TinyFaaSTest):
    fn = ""

    @classmethod
    def setUpClass(cls) -> None:
        super(TestEchoPY, cls).setUpClass()
        cls.fn = startFunction(path.join(fn_path, "echo-py"), "echo-py", "python3", 1)

    def setUp(self) -> None:
        super(TestEchoPY, self).setUp()
        self.fn = TestEchoPY.fn

    def test_invoke_http(self) -> None:
        """invoke a function"""

        # make a request to the function with a payload
        payload = "Hello World!"

        req = urllib.request.Request(
            f"http://{self.host}:{self.http_port}/{self.fn}",
            data=payload.encode("utf-8"),
        )

        res = urllib.request.urlopen(req, timeout=10)

        # check the response
        self.assertEqual(res.status, 200)
        self.assertEqual(res.read().decode("utf-8"), payload)

        return
class TestEchoJS(TinyFaaSTest):
    fn = ""

    @classmethod
    def setUpClass(cls) -> None:
        super(TestEchoJS, cls).setUpClass()
        cls.fn = startFunction(path.join(fn_path, "echo-js"), "echojs", "nodejs", 1)

    def setUp(self) -> None:
        super(TestEchoJS, self).setUp()
        self.fn = TestEchoJS.fn

    def test_invoke_http(self) -> None:
        """invoke a function"""

        # make a request to the function with a payload
        payload = "Hello World!"

        req = urllib.request.Request(
            f"http://{self.host}:{self.http_port}/{self.fn}",
            data=payload.encode("utf-8"),
            headers={
                "Content-Type": "text/plain"
            }
        )

        res = urllib.request.urlopen(req, timeout=10)

        # check the response
        self.assertEqual(res.status, 200)
        self.assertEqual(res.read().decode("utf-8"), payload)

        return
class TestEchoGo(TinyFaaSTest):
    fn = ""

    @classmethod
    def setUpClass(cls) -> None:
        super(TestEchoGo, cls).setUpClass()
        cls.fn = startFunction(path.join(fn_path, "echo-go"), "echo-go", "go", 1)

    def setUp(self) -> None:
        super(TestEchoGo, self).setUp()
        self.fn = TestEchoGo.fn

    def test_invoke_http(self) -> None:
        """invoke a function"""

        # make a request to the function with a payload
        payload = "Hello World!"

        req = urllib.request.Request(
            f"http://{self.host}:{self.http_port}/{self.fn}",
            data=payload.encode("utf-8"),
        )

        res = urllib.request.urlopen(req, timeout=10)

        # check the response
        self.assertEqual(res.status, 200)
        self.assertEqual(res.read().decode("utf-8"), payload)
        return
class TestBinary(TinyFaaSTest):
    fn = ""

    @classmethod
    def setUpClass(cls) -> None:
        super(TestBinary, cls).setUpClass()
        cls.fn = startFunction(
            path.join(fn_path, "echo-binary"), "echobinary", "binary", 1
        )

    def setUp(self) -> None:
        super(TestBinary, self).setUp()
        self.fn = TestBinary.fn

    def test_invoke_http(self) -> None:
        """invoke a function"""

        # make a request to the function with a payload
        payload = "Hello World!"

        req = urllib.request.Request(
            f"http://{self.host}:{self.http_port}/{self.fn}",
            data=payload.encode("utf-8"),
        )

        res = urllib.request.urlopen(req, timeout=10)

        # check the response
        self.assertEqual(res.status, 200)
        self.assertEqual(res.read().decode("utf-8"), payload)

        return
class TestShowHeadersJS(TinyFaaSTest):
    fn = ""

    @classmethod
    def setUpClass(cls) -> None:
        super(TestShowHeadersJS, cls).setUpClass()
        cls.fn = startFunction(
            path.join(fn_path, "show-headers-js"), "headersjs", "nodejs", 1
        )

    def setUp(self) -> None:
        super(TestShowHeadersJS, self).setUp()
        self.fn = TestShowHeadersJS.fn

    def test_invoke_http(self) -> None:
        """invoke a function"""

        # make a request to the function with a custom headers
        req = urllib.request.Request(
            f"http://{self.host}:{self.http_port}/{self.fn}",
            headers={"lab": "scalable_software_systems_group"},
        )

        res = urllib.request.urlopen(req, timeout=10)

        # check the response
        self.assertEqual(res.status, 200)
        response_body = res.read().decode("utf-8")
        response_json = json.loads(response_body)
        self.assertIn("lab", response_json)
        self.assertEqual(
            response_json["lab"], "scalable_software_systems_group"
        )  # custom header
        self.assertIn("user-agent", response_json)
        self.assertIn("Python-urllib", response_json["user-agent"])  # python client

        return
class TestShowHeaders(
    TinyFaaSTest
):  # Note: In Python, the http.server module (and many other HTTP libraries) automatically capitalizes the first character of each word in the header keys.
    fn = ""

    @classmethod
    def setUpClass(cls) -> None:
        super(TestShowHeaders, cls).setUpClass()
        cls.fn = startFunction(
            path.join(fn_path, "show-headers"), "headers", "python3", 1
        )

    def setUp(self) -> None:
        super(TestShowHeaders, self).setUp()
        self.fn = TestShowHeaders.fn

    def test_invoke_http(self) -> None:
        """invoke a function"""

        # make a request to the function with a custom headers
        req = urllib.request.Request(
            f"http://{self.host}:{self.http_port}/{self.fn}",
            headers={"Lab": "scalable_software_systems_group"},
        )

        res = urllib.request.urlopen(req, timeout=10)

        # check the response
        self.assertEqual(res.status, 200)
        response_body = res.read().decode("utf-8")
        response_json = json.loads(response_body)
        self.assertIn("Lab", response_json)
        self.assertEqual(
            response_json["Lab"], "scalable_software_systems_group"
        )  # custom header
        self.assertIn("User-Agent", response_json)
        self.assertIn("Python-urllib", response_json["User-Agent"])  # python client

        return

if __name__ == "__main__":
    # check that make is installed
    try:
        subprocess.run(["make", "--version"], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f"Make is not installed:\n{e.stderr.decode('utf-8')}")
        sys.exit(1)

    # check that Docker is working
    try:
        subprocess.run(["docker", "ps"], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f"Docker is not installed or not working:\n{e.stderr.decode('utf-8')}")
        sys.exit(1)

    unittest.main()  # run all tests
