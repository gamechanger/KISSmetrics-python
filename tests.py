from __future__ import with_statement

from cgi import parse_qsl
from contextlib import contextmanager
from cStringIO import StringIO
import errno
import os
import shutil
import sys
import unittest
import urllib
import urllib2
import urlparse

from mocker import Mocker

import km
from km import KM


class TestCase(unittest.TestCase):
    def setUp(self):
        super(TestCase, self).setUp()
        self.mocker = Mocker()
        reload(km)
        global KM
        KM = km.KM

    def assertStartsWith(self, string, prefix, msg=None):
        if msg is None:
            msg = '%r does not start with %r' % (string, prefix)
        self.assertTrue(string.startswith(prefix), msg)

    def assertEndsWith(self, string, suffix, msg=None):
        if msg is None:
            msg = '%r does not end with %r' % (string, suffix)
        self.assertTrue(string.endswith(suffix), msg)

    @contextmanager
    def get_log(self, log_name):
        try:
            with open(KM.log_name(log_name), 'r') as log_file:
                yield log_file
        except IOError, e:
            if e.errno == errno.ENOENT:
                yield None


class LogDir(object):
    def __init__(self, log_dir=None):
        if log_dir is None:
            log_dir = os.path.join(os.path.dirname(__file__), 'test_logs')
        self.log_dir = log_dir

    def __enter__(self):
        shutil.rmtree(self.log_dir, ignore_errors=True)
        os.mkdir(self.log_dir)
        return self.log_dir

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.log_dir, ignore_errors=True)


class StdIO(object):
    def __enter__(self):
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
        self.stdout = StringIO()
        self.stderr = StringIO()
        sys.stdout = self.stdout
        sys.stderr = self.stderr
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr


class TestInit(TestCase):
    def test_default(self):
        self.assertEqual(KM._id, None)
        self.assertEqual(KM.host, 'trk.kissmetrics.com:80')
        self.assertEqual(KM.log_dir, '/tmp')
        self.assertEqual(KM._key, None)
        self.assertEqual(KM._logs, {})
        self.assertEqual(KM._to_stderr, True)
        self.assertEqual(KM._use_cron, None)

    def test_init(self):
        self.assertRaises(TypeError, KM.init)
        KM.init('key1')
        self.assertEqual(KM._key, 'key1')
        KM.init(key='key2')
        self.assertEqual(KM._key, 'key2')
        KM.init('key', host='example.com:80')
        self.assertEqual(KM.host, 'example.com:80')
        KM.init('key', log_dir='/tmp/')
        self.assertEqual(KM.log_dir, '/tmp/')
        KM.init('key', use_cron=True)
        self.assertEqual(KM._use_cron, True)
        KM.init('key', to_stderr=False)
        self.assertEqual(KM._to_stderr, False)

    def test_log_dir(self):
        KM.init('key', to_stderr=False, log_dir='/invalid/')


class TestActions(TestCase):
    @contextmanager
    def assertQuery(self, type, data, update=True):
        host = 'example.com'
        _t = 1
        _k = 'key'
        _p = 'id'
        with LogDir() as log_dir:
            KM.init(_k, host=host, to_stderr=False, log_dir=log_dir,
                    use_cron=True)
            KM.identify(_p)
            # Ensure time.time() always returns the same value
            time = self.mocker.replace('time.time')
            time()
            self.mocker.result(_t)
            self.mocker.count(0, None)
            with self.mocker:
                yield
                with self.get_log('query') as log:
                    data = dict(data, _t=str(_t), _k=_k)
                    if update:
                        data['_p'] = _p
                    line = log.readline().strip()
                    bits = urlparse.urlsplit(line)
                    self.assertEqual(bits[2], '/%s' % urllib.quote(type))
                    self.assertEqual(dict(parse_qsl(bits[3])), data)
                    self.assertEqual(log.readline(), '')
                os.unlink(KM.log_name('query'))

    def test_alias(self):
        name = 'name'
        alias_to = 'alias_to'
        with self.assertQuery('a', {'_n': alias_to, '_p': name}, update=False):
            KM.alias(name, alias_to)

    def test_record(self):
        action = 'action'
        with self.assertQuery('e', {'_n': action}):
            KM.record(action)
        data = {'a': 'a'}
        with self.assertQuery('s', data):
            KM.record(data)

    def test_set(self):
        data = {'a': 'a'}
        with self.assertQuery('s', data):
            KM.set(data)

    def test_send_logged_queries(self):
        host = 'example.com'
        line = '/type?foo=1&bar=2'
        with LogDir() as log_dir:
            KM.init('key', host=host, log_dir=log_dir)
            # Write line to the query log
            KM.log_query(line)
            # Expect that line to be requested
            urlopen = self.mocker.replace('urllib2.urlopen')
            urlopen('http://%s%s' % (host, line))
            self.mocker.result(None)
            with self.mocker:
                KM.send_logged_queries()
                self.assertFalse(os.path.exists(KM.log_name('query')))
                self.assertFalse(os.path.exists(KM.log_name('send')))


class TestLog(TestCase):
    def setUp(self):
        reload(km)
        global KM
        KM = km.KM

    def test_log_name(self):
        log_dir = '/var/log'
        KM.init('key', to_stderr=False, log_dir=log_dir)
        self.assertEqual(KM.log_name('invalid'),
                         os.path.join(log_dir, ''))
        self.assertEqual(KM.log_name('error'),
                         os.path.join(log_dir, 'kissmetrics_error.log'))
        self.assertEqual(KM.log_name('query'),
                         os.path.join(log_dir, 'kissmetrics_query.log'))
        send_name = KM.log_name('send')
        self.assertStartsWith(send_name, log_dir)
        self.assertEndsWith(send_name, 'kissmetrics_sending.log')
        self.assertEqual(KM.log_name('send'), send_name)

    def test_log_query(self):
        with LogDir() as log_dir:
            KM.init('key', to_stderr=False, log_dir=log_dir)
            KM.log_query('Query')
            with self.get_log('query') as log:
                self.assertEndsWith(log.read(), 'Query\n')

    def test_log_send(self):
        with LogDir() as log_dir:
            KM.init('key', to_stderr=False, log_dir=log_dir)
            KM.log_send('Send')
            with self.get_log('send') as log:
                self.assertEndsWith(log.read(), 'Send\n')

    def test_log_error(self):
        with LogDir() as log_dir:
            KM.init('key', to_stderr=True, log_dir=log_dir)
            with StdIO() as stdio:
                KM.log_error('Error')
                self.assertEndsWith(stdio.stderr.getvalue(),
                                    '> Error\n')
            with self.get_log('error') as log:
                self.assertEndsWith(log.read(), '> Error\n')

    def test_log(self):
        with LogDir() as log_dir:
            KM.init('key', to_stderr=True, log_dir=log_dir)
            KM.log('error', 'Error')
            with self.get_log('error') as log:
                self.assertEndsWith(log.read(), 'Error\n')
            KM.log('query', 'Query')
            with self.get_log('query') as log:
                self.assertEndsWith(log.read(), 'Query\n')
            KM.log('send', 'Send')
            with self.get_log('send') as log:
                self.assertEndsWith(log.read(), 'Send\n')
            KM.log('invalid', 'Invalid')

    def test_log_dir_writable(self):
        log_dir = '/invalid/'
        with StdIO() as stdio:
            KM.init('key', to_stderr=True, log_dir=log_dir)
            self.assertEqual(stdio.stderr.getvalue(),
                             ("Couldn't open %(log_dir)skissmetrics_query.log "
                              "for writing. Does %(log_dir)s exist? "
                              "Permissions?\n" % {'log_dir': log_dir}))


class TestQuery(TestCase):
    def test_generate_query(self):
        host = 'example.com'
        _t = 1
        _k = 'key'
        _p = 'id'
        with LogDir() as log_dir:
            KM.init(_k, host=host, to_stderr=False, log_dir=log_dir,
                    use_cron=False)
            KM.identify(_p)
            # Ensure time.time() always returns the same value
            time = self.mocker.replace('time.time')
            time()
            self.mocker.result(_t)
            self.mocker.count(1, None)
            urlopen = self.mocker.replace('urllib2.urlopen')
            # First
            urlopen('http://%s/e?_t=%d&_k=%s' % (host, _t, _k))
            self.mocker.result(None)
            # Second
            urlopen('http://%s/e?_t=%d&_k=%s&_p=%s' % (host, _t, _k, _p))
            self.mocker.result(None)
            # Third
            urlopen('http://%s/f?_t=%d&_k=%s&_p=%s' % (host, _t, _k, _p))
            self.mocker.throw(urllib2.HTTPError('', 500, 'Error', None, None))
            with self.mocker:
                # First
                KM.generate_query('e', {}, update=False)
                # Second
                KM.generate_query('e', {}, update=True)
                # Third
                KM.generate_query('f', {}, update=True)
                with self.get_log('error') as log:
                    self.assertEndsWith(log.read(),
                                        '> HTTP Error 500: Error\n')

    def test_generate_query_cron(self):
        host = 'example.com'
        _t = 1
        _k = 'key'
        _p = 'id'
        with LogDir() as log_dir:
            KM.init(_k, host=host, to_stderr=False, log_dir=log_dir,
                    use_cron=True)
            KM.identify(_p)
            # Ensure time.time() always returns the same value
            time = self.mocker.replace('time.time')
            time()
            self.mocker.result(_t)
            self.mocker.count(1, None)
            with self.mocker:
                KM.generate_query('e', {}, update=False)
                KM.generate_query('e', {}, update=True)
                with self.get_log('query') as log:
                    self.assertEqual(log.readline(),
                                     '/e?_t=%s&_k=%s\n' % (_t, _k))
                    self.assertEqual(log.readline(),
                                     '/e?_t=%s&_k=%s&_p=%s\n' % (_t, _k, _p))
                    self.assertEqual(log.read(), '')

    def test_send_query(self):
        host = 'example.com'
        line = '/line?param=1'
        url = 'http://%s%s' % (host, line)
        with LogDir() as log_dir:
            KM.init('key', host=host, to_stderr=False, log_dir=log_dir)
            urlopen = self.mocker.replace('urllib2.urlopen')
            # First
            urlopen(url)
            self.mocker.result(None)
            # Second
            urlopen(url)
            self.mocker.throw(urllib2.HTTPError(url, 500, 'Error', None, None))
            with self.mocker:
                # First
                KM.send_query(line)
                # Second
                self.assertRaises(urllib2.HTTPError, KM.send_query, line)


class TestUtils(TestCase):
    def test_reset(self):
        with LogDir() as log_dir:
            KM.init('key', to_stderr=False, log_dir=log_dir)
            KM.identify('id')
            KM.log_query('Query')
            self.assertEqual(KM._key, 'key')
            self.assertEqual(KM._id, 'id')
            self.assertEqual(
                KM._logs,
                {'query': os.path.join(log_dir, 'kissmetrics_query.log')}
            )
        KM.reset()
        self.assertEqual(KM._key, None)
        self.assertEqual(KM._id, None)
        self.assertEqual(KM._logs, {})
        
    def test_is_identified(self):
        with LogDir() as log_dir:
            KM.log_dir = log_dir
            with StdIO() as stdio:
                self.assertFalse(KM.is_identified())
                KM.identify('id')
                self.assertTrue(KM.is_identified())

    def test_is_initialize_and_identified(self):
        with LogDir() as log_dir:
            KM.log_dir = log_dir
            with StdIO() as stdio:
                self.assertFalse(KM.is_initialized_and_identified())
                KM.init('key')
                self.assertFalse(KM.is_initialized_and_identified())
                KM.identify('id')
                self.assertTrue(KM.is_initialized_and_identified())
                KM.init(None)
                self.assertFalse(KM.is_initialized_and_identified())

    def test_is_initialized(self):
        with LogDir() as log_dir:
            KM.log_dir = log_dir
            with StdIO() as stdio:
                self.assertFalse(KM.is_initialized())
                KM.init('key')
                self.assertTrue(KM.is_initialized())


class TestHelpers(unittest.TestCase):
    def test_is_robot(self):
        from km.helpers import is_robot
        # Empty user agent
        self.assertFalse(is_robot(''))
        # Whitelist
        self.assertFalse(is_robot('Dillo/0.8.5'))
        # Blacklist
        self.assertTrue(is_robot('dCSbot/1.1'))
        # Opera
        self.assertFalse(is_robot('Opera/9.80'))
        # Mozilla
        self.assertFalse(is_robot('Mozilla/5.0 (X11; U; Linux i686; en-US)'))
        self.assertTrue(is_robot('Mozilla/5.0'))
        self.assertTrue(is_robot('Mozilla'))


class TestMain(TestCase):
    def test_args(self):
        from km import main
        with LogDir() as log_dir:
            host = 'host:80'
            km = self.mocker.replace('km.KM')
            # First
            km.init('key', log_dir=None, host=None)
            self.mocker.result(None)
            km.send_logged_queries()
            self.mocker.result(None)
            # Second
            km.init('key', log_dir=log_dir, host=None)
            self.mocker.result(None)
            km.send_logged_queries()
            self.mocker.result(None)
            # Third
            km.init('key', log_dir=log_dir, host=host)
            self.mocker.result(None)
            km.send_logged_queries()
            self.mocker.result(None)
            with self.mocker.order():
                with StdIO() as stdio:
                    self.assertEqual(main('km'), 1)
                    self.assertStartsWith(stdio.stderr.getvalue(),
                                          'At least one argument required. ')
                # First
                self.assertEqual(main('km', 'key'), 0)
                # Second
                self.assertEqual(main('km', 'key', log_dir), 0)
                # Third
                self.assertEqual(main('km', 'key', log_dir, host), 0)


if __name__ == '__main__':
    unittest.main()
