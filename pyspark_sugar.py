"""
Set python traceback on dataframe actions, enrich spark UI with actual business logic stages of spark application.
"""
import collections
import contextlib
import functools
import inspect
import os
import traceback
from unittest import mock

import pyspark

_DATAFRAME_ACTIONS = [
    pyspark.sql.DataFrame.collect,
    pyspark.sql.DataFrame.count,
    pyspark.sql.DataFrame.toLocalIterator,
    pyspark.sql.DataFrame.show,
    pyspark.sql.DataFrame.toPandas,
    pyspark.sql.DataFrame.checkpoint,
    pyspark.sql.DataFrame.localCheckpoint,
]

_RDD_ACTIONS = [
    pyspark.RDD.take,
    pyspark.RDD.count,
]


@contextlib.contextmanager
def job_description(description, parent=True):
    """
    Set job description in spark ui.

    :param parent: Prepend with parent job description
    """
    description = str(description)

    sc = pyspark.SparkContext.getOrCreate()  # type: pyspark.SparkContext

    prev_description = sc.getLocalProperty('spark.job.description')

    if parent and prev_description:
        description = str(prev_description) + ' -> ' + description

    sc.setJobDescription(description)
    try:
        yield
    finally:
        sc.setJobDescription(None)


def job_description_decor(description: str):
    """Set job description for underlying function"""

    def decor(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with job_description(description):
                return func(*args, **kwargs)

        return wrapper

    return decor


@contextlib.contextmanager
def job_group(group_id, description=None):
    """Set current job group"""
    sc = pyspark.SparkContext.getOrCreate()  # type: pyspark.SparkContext
    sc.setJobGroup(str(group_id), description)
    try:
        yield
    finally:
        sc.cancelJobGroup(str(group_id))


def job_group_decor(group_id, description=None):
    """Set current job group for underlying function"""

    def decor(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with job_group(group_id, description):
                return func(*args, **kwargs)

        return wrapper

    return decor


def _get_traceback(frame):
    return ''.join(traceback.format_stack(f=frame))


CallSite = collections.namedtuple('CallSite', ('short', 'long'))


def _get_callsite(func) -> CallSite:
    frame_info = None

    frames = inspect.getouterframes(inspect.currentframe())

    pyspark_path = os.path.dirname(pyspark.__file__)

    if frames:
        for i in range(0, len(frames)):
            filename = frames[i].filename
            if filename != __file__ and not filename.startswith(pyspark_path):
                frame_info = frames[i]
                break

    if frame_info:
        return CallSite(
            short='%s at %s:%s' % (func.__name__, frame_info.filename, frame_info.lineno),
            long=_get_traceback(frame_info.frame)
        )


class SCCallSiteSync:
    _spark_stack_depth = 0
    _callsite_long = None
    _callsite_short = None

    def __init__(self, sc, func):
        self._context = sc
        self._callsite = _get_callsite(func)

    def __enter__(self):
        if SCCallSiteSync._spark_stack_depth == 0 and self._callsite:
            self._context.setLocalProperty('callSite.long', self._callsite.long)
            self._context.setLocalProperty('callSite.short', self._callsite.short)
        SCCallSiteSync._spark_stack_depth += 1

    def __exit__(self, type, value, tb):
        SCCallSiteSync._spark_stack_depth -= 1
        if SCCallSiteSync._spark_stack_depth == 0:
            self._context._jsc.clearCallSite()


@contextlib.contextmanager
def SCCallSiteSyncDummy(*args, **kwargs):
    yield


def _action_wrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        sc = pyspark.SparkContext.getOrCreate()
        with SCCallSiteSync(sc, func=func):
            return func(*args, **kwargs)

    return wrapper


@contextlib.contextmanager
def patch_dataframe_actions():
    """Patch almost-all dataframe and rdd actions that will set python stack trace into action job details"""
    with contextlib.ExitStack() as stack, mock.patch('pyspark.sql.dataframe.SCCallSiteSync', new=SCCallSiteSyncDummy):
        for func in _DATAFRAME_ACTIONS:
            stack.enter_context(mock.patch.object(pyspark.sql.DataFrame, func.__name__, new=_action_wrapper(func)))

        for func in _RDD_ACTIONS:
            stack.enter_context(mock.patch.object(pyspark.RDD, func.__name__, new=_action_wrapper(func)))

        yield
