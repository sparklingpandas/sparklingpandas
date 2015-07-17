"""
Tests for the PandaSpark module, you probably don't want to import this
directly.
"""
import subprocess as sub

try:
    sub.check_output("source ./sparklingpandas/test/resources/setup-test-env"
                     + ".sh", shell=True,
                        stderr=sub.STDOUT)
except sub.CalledProcessError, err:
        print "The failed test setup command was [%s]." % err.cmd
        print "The output of the command was [%s]" % err.output
        raise