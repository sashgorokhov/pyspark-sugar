"""
Set python traceback on dataframe actions, enrich spark UI with actual business logic stages of spark application.
"""
import contextlib
import functools
import inspect
import traceback
from unittest import mock

import pyspark


_DATAFRAME_ACTIONS = [
    pyspark.sql.DataFrame.collect,
    pyspark.sql.DataFrame.count,
    pyspark.sql.DataFrame.toLocalIterator,
    pyspark.sql.DataFrame.show,
    pyspark.sql.DataFrame.toPandas,
]


_RDD_ACTIONS = [
    pyspark.RDD.take,
    pyspark.RDD.count,
    pyspark.RDD.collect,
]


@contextlib.contextmanager
def job_description(description):
    """
    Set job description in spark ui.

    :param parent: Prepend with parent job description
    """
    description = str(description)

    sc = pyspark.SparkContext.getOrCreate()  # type: pyspark.SparkContext

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
        sc.setJobGroup(None, None)


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


def _action_wrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        sc = pyspark.SparkContext.getOrCreate()  # type: pyspark.SparkContext
        f = inspect.getouterframes(inspect.currentframe())[1][0]
        prev_callsite_long = sc.getLocalProperty('callSite.long')
        prev_callsite_short = sc.getLocalProperty('callSite.short')
        sc.setLocalProperty('callSite.long', _get_traceback(f))
        sc.setLocalProperty('callSite.short', '%s at %s:%s' % (func.__name__, f.f_code.co_filename, f.f_lineno))
        try:
            return func(*args, **kwargs)
        finally:
            sc.setLocalProperty('callSite.long', prev_callsite_long)
            sc.setLocalProperty('callSite.short', prev_callsite_short)

    return wrapper


@contextlib.contextmanager
def patch_dataframe_actions():
    """Patch almost-all dataframe and rdd actions that will set python stack trace into action job details"""
    with contextlib.ExitStack() as stack:
        for func in _DATAFRAME_ACTIONS:
            stack.enter_context(mock.patch.object(pyspark.sql.DataFrame, func.__name__, new=_action_wrapper(func)))

        for func in _RDD_ACTIONS:
            stack.enter_context(mock.patch.object(pyspark.RDD, func.__name__, new=_action_wrapper(func)))

        yield
