import tempfile
import unittest
from pathlib import Path
from run import verify_logs


def message(version, kind, ident, sender, target):
    body = f"35={kind}\x0149={sender}\x0156={target}\x01112=PT{ident}\x01".encode()
    data = b"8=" + version.encode() + b"\x019=" + str(len(body)).encode() + b"\x01" + body
    return data + f"10={sum(data) % 256:03}\x01\n".encode()


class ReceiptTest(unittest.TestCase):
    def test_wrong_session_or_duplicate_is_rejected_even_on_both_peers(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            for role, ids in (("client", "BANZAI-EXEC"), ("server", "EXEC-BANZAI")):
                logs = out / f"logs-{role}"
                logs.mkdir()
                for i, v in enumerate(("FIX.4.2", "FIX.4.4")):
                    request = message(v, 1, i, "BANZAI", "EXEC")
                    reply = message(v, 0, i, "EXEC", "BANZAI")
                    (logs / f"{v}-{ids}.messages.log").write_bytes(request + reply)
            self.assertEqual(4, len(verify_logs(out, 2)))
            for bad in [
                request,
                request + reply + reply,
                request + message(v, 0, i, "OTHER", "BANZAI"),
            ]:
                for f in out.glob("logs-*/FIX.4.4-*.messages.log"):
                    f.write_bytes(bad)
                with self.assertRaises(ValueError):
                    verify_logs(out, 2)
