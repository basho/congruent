#!/usr/bin/env python
from __future__ import print_function
from riak import RiakClient, RiakObject, RiakBucket
from base64 import b64decode
try:
    import json
except ImportError:
    import simplejson as json

from argparse import ArgumentParser, FileType

UNIMPLEMENTED = 'unimplemented'
NOTFOUND = 'not_found'

class Commands:
    def get(self, options):
        """
        Performs a 'get' request on a key in the given bucket.
        """
        (bucket, key) = self.extract_bkey(options)
        robj = self.client.bucket(bucket).get_binary(key, options.get('r'))
        if robj.has_siblings():
            data = []
            for sibling in robj.get_siblings():
                data.append(sibling.get_data())
            return dict(siblings = data)
        elif robj.exists():
            return dict(siblings = [ robj.get_data() ])
        else:
            return NOTFOUND

    def put(self, options):
        """
        Performs a 'put' request on a key in the given bucket
        """
        (bucket, key, value) = self.extract_bkey_value(options)
        robj = self.client.bucket(bucket).get_binary(key, options.get('r'))
        robj.set_data(value)
        if 'content_type' in options:
            robj.set_content_type(options.pop('content_type'))
        result = robj.store(w=options.get('w'), dw=options.get('dw'), return_body=options.get('return_body'))
        if options.get('returnbody'):
            if robj.has_siblings():
                data = []
                for sibling in robj.get_siblings():
                    data.append(sibling.get_data())
                return dict(siblings = data)
            else:
                return dict(siblings = [ robj.get_data() ])
        else:
            return True
    
    def delete(self, options):
        (bucket, key) = self.extract_bkey(options)
        result = RiakObject(self.client, self.client.bucket(bucket), key).delete(rw=options.get('rw'))
        return True

    def keys(self, options):
        bucket = self.client.bucket(self.extract_bucket(options))
        result = self.client.get_transport().get_keys(bucket)
        return dict(keys = result)
        
    def ping(self, options):
        return self.client.is_alive()

    def extract_bkey_value(self, options):
        """
        Extracts the bucket, key, and value from the command options
        """
        (bucket, key) = self.extract_bkey(options)
        value = b64decode(options.pop('value').encode('ascii', 'ignore'))
        return (bucket, key, value)

    def extract_bkey(self, options):
        """
        Extracts the bucket and key from the command options
        """
        return (self.extract_bucket(options),
                b64decode(options.pop('key').encode('ascii', 'ignore')))

    def extract_bucket(self, options):
        """
        Extracts the bucket from the command options
        """
        return b64decode(options.pop('bucket').encode('ascii', 'ignore'))

class Runner(Commands):
    """
    Runs client commands sent to it by the Congruent testing framework.
    """

    def run(self):
        """
        Runs the commands in the input file, writing results to the output file.
        """
        (host, http_port, pb_port) = self.h.split(':')
        self.client = RiakClient(host = host, port = http_port)
        commands = json.load(self.f)
        results = []
        errors = 0
        for command in commands:
            name = command.pop('command')
            try:
                if hasattr(self, name):
                    method = getattr(self, name)
                    results.append(method(command))
                else:
                    errors = errors + 1
                    results.append(UNIMPLEMENTED)
            except Exception as error:
                errors = errors + 1
                results.append(dict(error = str(error)))
        self.report(results)
        return errors

    def report(self, results):
        """
        Writes the results of the run commands into the output file.
        """
        f = self.o
        for result in results:
            if result is UNIMPLEMENTED:
                print('{error, unimplemented}.', file=f)
            elif result is True:
                print('ok.', file=f)
            elif result is NOTFOUND:
                print('{ok, not_found}.', file=f)
            elif 'error' in result:
                print('{error, %r}.' % result['error'], file=f)
            elif 'keys' in result:
                keystrings = map(self.to_quoted_string, result['keys'])
                print('{ok, [%s]}.' % ','.join(keystrings), file=f)
            elif 'siblings' in result:
                siblings = map(self.to_binary, result['siblings'])
                print('{ok, [%s]}.' % ','.join(siblings), file=f)
            else:
                print('{error, %s}.' % self.to_quoted_string(str(result)), file=f)
        f.close()

    def to_quoted_string(self, theString):
        return '"%s"' % theString.replace('"', '\\"')

    def to_binary(self, value):
        strord = lambda x: str(ord(x))
        chars = map(strord, str(value))
        return '<<%s>>' % ','.join(chars)

if __name__ == "__main__":
    getopt = ArgumentParser(add_help=False)
    getopt.add_argument('-f', type=FileType('r'), metavar="FILE", required=True)
    getopt.add_argument('-h', metavar="HOST:HTTP:PBC", required=True)
    getopt.add_argument('-o', type=FileType('w'), metavar="OUTFILE")

    runner = Runner()
    getopt.parse_args(namespace=runner)

    if runner.f is not None and runner.o is None:
        runner.o = open('%s.out' % runner.f.name, 'w')

    if runner.f is None or runner.o is None or runner.h is None:
        getopt.print_help()
        print()
        print("Missing mandatory arguments!")
        exit(127)

    exit(runner.run())
