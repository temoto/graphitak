import functools
import random

import db_psycopg2


def memoize():
    miss = object()
    cache = {}

    def wrapper(fun):
        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            cache_key = repr(args) + repr(kwargs)
            result = cache.get(cache_key, miss)
            if result is miss:
                if len(cache) > 1000:
                    cache.clear()
                result = cache[cache_key] = fun(*args, **kwargs)
            return result

        return wrapped
    return wrapper


def first(xs, cond):
    for x in xs:
        if cond(x):
            return x
    return None


def myrexp(m, k1, k2):
    sql = 'select myrexp(%s, %s, %s)::int8'
    return db_psycopg2.execute(sql, [m, k1, k2]).scalar()


@memoize()
def begin_all():
    sql = '''select w1freq, w1lower from (select distinct on (w1lower) w1lower, w1freq, t
from sentence where w1freq is not null order by w1lower, w1freq desc) q1
order by w1freq desc'''
    return db_psycopg2.execute(sql).fetchall()


@memoize()
def word3(w1, limit):
    sql = '''select freq, w1, w2, w3
from word3 where w1 = %s order by freq desc limit %s'''
    return db_psycopg2.execute(sql, [w1, limit]).fetchall()


def begin():
    bs = begin_all()
    bmax = bs[0]
    bmin = bs[-1]
    r = myrexp(bmax.w1freq * 0.5, 0.01, 2) + bmin.w1freq
    word = first(bs, lambda b: b.w1freq < r).w1lower
    return word


def path(w1, length):
    ws = [w1]
    while len(ws) < length:
        branches = word3(ws[-1], 1000)
        if not branches:
            break
        chosen = random.choice(branches)
        ws.append(chosen.w2)
        ws.append(chosen.w3)
    return ws


def phrase(length):
    w1 = begin()
    ws = path(w1, length)
    text = u' '.join(ws).capitalize() + u'.'
    return text


def paragraph(phrase_length, paragraph_length):
    ps = []
    while len(ps) < paragraph_length:
        ps.append(phrase(phrase_length))
    return u' '.join(ps)
