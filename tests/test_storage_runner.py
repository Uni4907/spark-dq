"""Bounded subprocess cleanup and credential-safe diagnostics; no Docker."""
import sys,tempfile,time,unittest
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'infrastructure'))
import storage as st

class StorageRunnerTest(unittest.TestCase):
    def test_timeout_kills_child_and_returns_within_bound(self):
        started=time.monotonic()
        with self.assertRaises(st.StorageError):
            st.run([sys.executable,'-c','import time;time.sleep(10)'],timeout=.05)
        self.assertLess(time.monotonic()-started,3)
    def test_interrupt_kills_owned_child_and_uses_bounded_reap(self):
        child=Mock();child.communicate.side_effect=[InterruptedError('signal'),(b'',b'')]
        with patch.object(st.subprocess,'Popen',return_value=child):
            with self.assertRaises(st.StorageError):st.run(['program','operation'])
        child.kill.assert_called_once();self.assertEqual(child.communicate.call_args.kwargs,{'timeout':5})
    def test_stderr_is_not_in_exception(self):
        with self.assertRaises(st.StorageError) as caught:
            st.run([sys.executable,'-c',"import sys;sys.stderr.write('synthetic-secret');sys.exit(2)"])
        self.assertNotIn('synthetic-secret',str(caught.exception))
    def test_output_stream_and_input_are_preserved(self):
        with tempfile.TemporaryFile() as stream:
            result=st.run([sys.executable,'-c','import sys;sys.stdout.buffer.write(sys.stdin.buffer.read())'],data=b'payload',output=stream)
            self.assertIsNone(result);stream.seek(0);self.assertEqual(stream.read(),b'payload')
        self.assertEqual(st.run([sys.executable,'-c',"print('ok')"]).strip(),b'ok')
    def test_missing_executable_has_safe_error(self):
        with self.assertRaises(st.StorageError):st.run(['/nonexistent-sparkdq-synthetic-program','unused'])

if __name__=='__main__':unittest.main()
