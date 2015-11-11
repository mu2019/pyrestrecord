#!/usr/bin/env python
# -*- coding: utf-8 -*-

import restrecord 

a=restrecord.Meta()
a.a = 'abc'
a.b = 1

print(a['a'],a.a,a.b)

del a.a

print(a.a)
print(dir(a))

print('record')

rc = restrecord.Record((['a','b'],[1,2]))
print(rc.keys())
print(rc[0],rc.a
