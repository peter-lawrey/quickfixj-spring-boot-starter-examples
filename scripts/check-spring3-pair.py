"""Start the packaged Spring 3 examples and verify all six QuickFIX/J logons."""
from contextlib import ExitStack
from pathlib import Path
import os
import re
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def stop(process):
    process.terminate()
    try:
        process.wait(timeout=12)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def check(directory):
    with ExitStack() as running, ExitStack() as sockets:
        ports = {}
        for default in range(9876, 9882):
            sock = sockets.enter_context(socket.socket())
            sock.bind(("127.0.0.1", 0))
            ports[default] = sock.getsockname()[1]
        processes = []
        logs = []
        for side in ("server", "client"):
            module = ROOT / f"simple-{side}-spring-3"
            config = (module / f"src/main/resources/quickfixj-{side}.cfg").read_text()
            for old, new in ports.items():
                config = re.sub(rf"(Port=){old}$", rf"\g<1>{new}", config, flags=re.M)
            path = directory / f"{side}.cfg"
            path.write_text(config)
            versions = re.findall(r"^BeginString=(.+)$", config, flags=re.M)
            if len(set(versions)) != 6:
                raise ValueError(f"{side}: expected six distinct sessions")
            pattern = "Received logon" if side == "client" else "Responding to Logon request"
            ids = "BANZAI-EXEC" if side == "client" else "EXEC-BANZAI"
            logs.extend((directory / f"logs-{side}/{v}-{ids}.event.log", pattern) for v in versions)
            env = dict(os.environ, QUICKFIXJ_MESSAGE_LOG_LEVEL="WARN")
            env[f"QUICKFIXJ_{side.upper()}_CONFIG_FILE"] = path.as_uri()
            output = running.enter_context((directory / f"{side}.log").open("w"))
            if side == "server":
                sockets.close()
            process = subprocess.Popen(
                ["java", "-jar", str(module / f"target/{module.name}.jar"), "--server.port=0"],
                cwd=directory, env=env, stdout=output, stderr=subprocess.STDOUT)
            running.callback(stop, process)
            processes.append(process)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and all(p.poll() is None for p in processes):
            if all(path.exists() and text in path.read_text() for path, text in logs):
                print("Six FIX sessions logged on at both peers")
                return
            time.sleep(0.2)
        for side in ("server", "client"):
            print((directory / f"{side}.log").read_text()[-3000:])
        raise AssertionError("Both peers must confirm all six logons")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="quickfixj-pair-") as tmp:
        check(Path(tmp))
