import re
import subprocess

import luigi

def shellout(template, **kwargs):

    """
    Takes a shell command template and executes it. The template must use
    the new (2.6+) format mini language. `kwargs` must contain any defined
    placeholder, only `output` is optional.
    Raises RuntimeError on nonzero exit codes.

    Simple template:

    wc -l < {input} > {output}

    Quoted curly braces:

    ps ax|awk '{{print $1}}' > {output}

    Usage with luigi:

    ...
    tmp = shellout('wc -l < {input} > {output}', input=self.input().fn)
    luigi.File(tmp).move(self.output.fn())
    ....

    """
    preserve_spaces = kwargs.get('preserve_spaces', False)
    stopover = luigi.File(is_tmp=True)  # Should return a random path string, e.g. /tmp/as3as8d90a8s9f8d
    if not 'output' in kwargs:
        kwargs.update({'output': stopover.fn})
    command = template.format(**kwargs)
    if not preserve_spaces:
        command = re.sub(' +', ' ', command)
    # logger.debug(cyan(command))
    code = subprocess.call([command], shell=True)
    if not code == 0:
        raise RuntimeError('%s exitcode: %s' % (command, code))
    # return kwargs.get('output')
    return stopover if stopover else luigi.File(kwargs.get('output'))


def shellout_no_stdout(template, **kwargs):

    """
    Takes a shell command template and executes it. The template must use
    the new (2.6+) format mini language. `kwargs` must contain any defined
    placeholder, and no output redirection is specified.
    Raises RuntimeError on nonzero exit codes.

    Simple template:

    wc -l < {input}

    Quoted curly braces:

    ps ax|awk '{{print $1}}'

    Usage with luigi:

    ...
    tmp = shellout('wc -l < {input}', input=self.input().fn)
    luigi.File(tmp).move(self.output.fn())
    ....

    """
    preserve_spaces = kwargs.get('preserve_spaces', False)
    command = template.format(**kwargs)
    if not preserve_spaces:
        command = re.sub(' +', ' ', command)
    # logger.debug(cyan(command))
    code = subprocess.call([command], shell=True)
    if not code == 0:
        raise RuntimeError('%s exitcode: %s' % (command, code))
    # return kwargs.get('output')
