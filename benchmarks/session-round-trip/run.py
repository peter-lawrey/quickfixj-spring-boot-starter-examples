"""Start the Spring Boot 3 pair and time two round-robin FIX sessions with public JLBH."""

import argparse
import csv
import hashlib
import json
import os
import re
import socket
import subprocess
import time
import zipfile
from contextlib import ExitStack
from pathlib import Path
from percentiles import summarize

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
FLAGS = ["-Xms512m", "-Xmx512m", "-XX:ActiveProcessorCount=4", "-XX:+UseG1GC"]
OPENS = [
    "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
]


def client_classpath(jar_path, out):
    classes = out / "observer"
    with zipfile.ZipFile(jar_path) as jar:
        for entry in jar.namelist():
            if entry.startswith(("BOOT-INF/classes/", "BOOT-INF/lib/")):
                jar.extract(entry, classes)
    benchmark_cp = (HERE / "target/benchmark.cp").read_text().strip()
    cp = os.pathsep.join(
        map(str, (classes, classes / "BOOT-INF/classes", classes / "BOOT-INF/lib/*", benchmark_cp))
    )
    subprocess.run(
        [
            "javac",
            "-proc:none",
            "--release",
            "17",
            "-cp",
            cp,
            "-d",
            str(classes),
            str(HERE / "SessionRoundTrip.java"),
        ],
        check=True,
    )
    return cp


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(12)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def verify_logs(out, total):
    receipts = {}
    client_hashes = {}
    for role, peers in (("client", "BANZAI-EXEC"), ("server", "EXEC-BANZAI")):
        for lane, version in enumerate(("FIX.4.2", "FIX.4.4")):
            log = out / f"logs-{role}/{version}-{peers}.messages.log"
            counts = [bytearray(total), bytearray(total)]
            digest = hashlib.sha256()
            streams = [hashlib.sha256(), hashlib.sha256()]
            with log.open("rb") as f:
                for line in f:
                    digest.update(line)
                    match = re.search(rb"\x01112=PT(\d+)\x01", line)
                    if not match:
                        continue
                    i = int(match[1])
                    frame = line.rstrip(b"\r\n")
                    begin, length, rest = frame.split(b"\x01", 2)
                    body, trailer = rest[:-7], rest[-7:]
                    if (
                        begin != b"8=" + version.encode()
                        or length != b"9=" + str(len(body)).encode()
                        or not re.fullmatch(rb"10=\d{3}\x01", trailer)
                        or sum(frame[:-7]) % 256 != int(trailer[3:6])
                    ):
                        raise ValueError(f"Invalid FIX frame in {log}")
                    fields = [field.split(b"=", 1) for field in body[:-1].split(b"\x01")]
                    tags = dict(fields)
                    kind = tags.get(b"35")
                    if (
                        len(tags) != len(fields)
                        or kind not in (b"0", b"1")
                        or i >= total
                        or i % 2 != lane
                    ):
                        raise ValueError(f"Unexpected frame in {log}, ID {i}")
                    sender, target = (b"BANZAI", b"EXEC") if kind == b"1" else (b"EXEC", b"BANZAI")
                    if (tags.get(b"49"), tags.get(b"56")) != (sender, target):
                        raise ValueError(f"Incorrect CompIDs in {log}, ID {i}")
                    counts[int(kind)][i] += 1
                    streams[int(kind)].update(frame)
            hashes = [stream.hexdigest() for stream in streams]
            if role == "client":
                client_hashes[version] = hashes
            elif hashes != client_hashes[version]:
                raise ValueError(f"Peer message bytes differ for {version}")

            if any(any(n != (i % 2 == lane) for i, n in enumerate(side)) for side in counts):
                raise ValueError(f"Missing or duplicate request/heartbeat in {log}")
            receipts[log.name] = {
                "sha256": digest.hexdigest(),
                "request_sha256": hashes[1],
                "heartbeat_sha256": hashes[0],
                "requests": sum(counts[1]),
                "heartbeats": sum(counts[0]),
            }
    return receipts


def samples(path):
    with path.open() as f:
        for row in csv.DictReader(f, delimiter="\t"):
            i = int(row["id"])
            yield i, i % 2, int(row["due_ns"]), int(row["ack_ns"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rate", type=int, default=20000)
    parser.add_argument("--seconds", type=int, default=5)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=12000)
    parser.add_argument("--logging", choices=("INFO", "WARN"), default="WARN")
    parser.add_argument("--dispatch", choices=("shared", "session"), default="shared")
    args = parser.parse_args()
    count = args.rate * args.seconds
    if min(args.rate, args.warmup, args.runs, args.seconds) < 1 or count % 2:
        parser.error("Use positive workload settings and an even number of messages per window")
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    with ExitStack() as ctx, ExitStack() as ports:
        replacements = {}
        for original in range(9876, 9882):
            sock = ports.enter_context(socket.socket())
            sock.bind(("127.0.0.1", 0))
            replacements[str(original)] = str(sock.getsockname()[1])
        for role in ("server", "client"):
            module = ROOT / f"simple-{role}-spring-3"
            packaged = module / f"target/{module.name}.jar"
            config = (module / f"src/main/resources/quickfixj-{role}.cfg").read_text()
            config = config.replace("[default]", "[default]\nFileLogHeartbeats=Y")
            config = re.sub(
                r"(Port=)(987[6-9]|988[01])$", lambda m: m[1] + replacements[m[2]], config, flags=re.M
            )
            cfg = out / f"{role}.cfg"
            cfg.write_text(config)
            env = dict(os.environ, QUICKFIXJ_MESSAGE_LOG_LEVEL=args.logging)
            env[f"QUICKFIXJ_{role.upper()}_CONFIG_FILE"] = cfg.as_uri()
            if role == "client":
                env["QUICKFIXJ_CLIENT_CONCURRENT_ENABLED"] = (
                    "true" if args.dispatch == "session" else "false"
                )
            if role == "server":
                ports.close()
                cmd = ["java", *FLAGS, "-jar", str(packaged), "--server.port=0"]
            else:
                cmd = [
                    "java",
                    *FLAGS,
                    *OPENS,
                    "-cp",
                    client_classpath(packaged, out),
                    "SessionRoundTrip",
                    str(args.warmup),
                    str(args.rate),
                    str(count),
                    str(args.runs),
                ]
            stdout = ctx.enter_context((out / f"{role}.log").open("w"))
            proc = subprocess.Popen(cmd, cwd=out, env=env, stdout=stdout, stderr=subprocess.STDOUT)
            ctx.callback(stop, proc)
            if role == "server":
                deadline = time.monotonic() + 45
                ready = out / "logs-server/FIX.4.4-EXEC-BANZAI.event.log"
                while not ready.exists() and proc.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.1)
                if proc.poll() is not None or not ready.exists():
                    raise RuntimeError(f"Server failed to start, see {out}/server.log")
            else:
                if proc.wait(timeout=180 + args.runs * args.seconds + args.warmup / args.rate):
                    raise RuntimeError(f"Client failed, see {out}/client.log")
    receipts = verify_logs(out, args.warmup + count * args.runs)
    (out / "wire-receipts.json").write_text(json.dumps(receipts, indent=2) + "\n")
    summarize(samples(out / "samples.tsv"), out, args.warmup, args.rate, count, args.runs)


if __name__ == "__main__":
    main()
