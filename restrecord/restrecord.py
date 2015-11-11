#!/usr/bin/env python
# -*- coding: utf-8 -*-
try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict

class Meta(object):
    def __init__(self,read_only=False):
        self.__dict__['__data__']={}
        self.__dict__['ReadOnly']=read_only

    def __setitem__(self,item,value):
        self.__dict__['__data__'][item]=value

    def __getitem__(self,item):
        return self.__dict__['__data__'][item]

    def __getattr__(self,attr):
        return self.__dict__['__data__'].get(attr)

    def __setattr__(self,attr,value):
        if self.isReadOnly(): #__dict__['__data__'].get('ReadOnly') == True:
            return
        self.__dict__['__data__'][attr]=value

    def __delitem__(self,attr):
        self.__dict__['__data__'].pop(attr)

    def __delattr__(self,attr):
        self.__dict__['__data__'].pop(attr)

    def isReadOnly(self):
        return self.__dict__['__data__'].get('ReadOnly') == True

    def keys(self):
        return self.__dict__['__data__'].keys()

    def values(self):
        return self.__dict__['__data__'].values()

    def items(self):
        return self.__dict__['__data__'].items()

    def update(self,adict):
        if self.isReadOnly(): #__dict__['__data__'].get('ReadOnly') == True:
            return
        self.__dict__['__data__'].update(adict)
        

class Record(Meta):
    def __init__(self,data,read_only=False,dot='_'):
        '''
        data: dict or OrderedDict or ([keys],[values])
        dot: 字段名称中含点号(.)时,用属性访问会用下横线代替
        '''
        super(Record,self).__init__(read_only)
        if isinstance(data,(dict,OrderedDict)):
            d = OrderedDict(data)
        elif isinstance(data,Record):
            d = data.__dict__['__data__']
        elif isinstance(data,(list,tuple)) and \
             isinstance(data[0],(list,tuple)) and \
             isinstance(data[1],(list,tuple))  :
            mlen = min(len(data[0]),len(data[1]))
            d = OrderedDict(zip(data[0][:mlen],data[1][:mlen]))
        self._Dot = dot
        self.__dict__['__data__'] = d
        self._Keys = list(d.keys())

    def __dict__(self):
        return dict(self.__dict__['__data__'])
        
    def __setitem__(self,item,value):
        if self.isReadOnly():
            return
        if isinstance(item,int):
            self.__dict__['__data__'][self._Keys[item]] = value
        else:
            k = item
            while self._Dot in k and k not in self._Keys:
                k = k.replace(self._Dot,'.',1)
            if k in self._Keys:
                self.__dict__['__data__'][item] = value

    def __getitem__(self,item):
        if isinstance(item,int):
            return self.__dict__['__data__'][self._Keys[item]]
        return self.__dict__['__data__'][item]

    def __getattr__(self,attr):
        k = attr
        while self._Dot in k and k not in self._Keys:
            k = k.replace(self._Dot,'.',1)
        return self.__dict__['__data__'].get(k)

    def __len__(self):
        return len(self._Keys)

    def __setattr__(self,attr,value):
        if self.isReadOnly():
            return 
        k = attr
        while self._Dot in k and k not in self._Keys:
            k = k.replace(self._Dot,'.',1)
        if k in self._Keys:
            self.__dict__['__data__'][k]=value

    def keys(self):
        return self._Keys

    def values(self):
        return self.__dict__['__data__'].values()

    def load(self,data):
        '''
        load data in Record
        data: data ['__RECORD__',dict or OrderedDict or ([keys],[values])]
        '''
        if data[0] != '__RECORD__':
            raise Exception("'__RECORD__' must at loading data list/tupe  first item.")

        if isinstance(data[1],(dict,OrderedDict)):
            d = OrderedDict(data[1])
        elif isinstance(data[1],Record):
            d = data[1].__dict__['__data__']
        elif isinstance(data[1],(list,tuple)) and \
             isinstance(data[1][0],(list,tuple)) and \
             isinstance(data[1][1],(list,tuple))  :
            mlen = min(len(data[1][0]),len(data[1][1]))
            d = OrderedDict(zip(data[1][0][:mlen],data[1][1][:mlen]))
        self.__dict__['__data__'] = d
        self._Keys = list(d.keys())
        

class TransRecord(Record):
    def __init__(self,data,read_only=False,dot='_'):
        super(TransRecord,self).__init__(read_only)
        self._Dirty = Meta()

    def __setattr__(self,attr,value):
        if self.isReadOnly():
            return 
        k = attr
        while self._Dot in k and k not in self._Keys:
            k = k.replace(self._Dot,'.',1)
        if k in self._Keys:
            self._Dirty[k] = self.__dict__['__data__'][k]
            self.__dict__['__data__'][k]=value
        
    def __setitem__(self,item,value):
        if self.isReadOnly():
            return
        if isinstance(item,int):
            self.__dict__['__data__'][self._Keys[item]] = value
        else:
            k = item
            while self._Dot in k and k not in self._Keys:
                k = k.replace(self._Dot,'.',1)
            if k in self._Keys:
                self._Dirty[k] = self.__dict__['__data__'][item]
                self.__dict__['__data__'][item] = value

    def rollback(self):
        for k,v in self._Dirty.items():
            self.__dict__['__data__'][k] = v

    def commit(self):
        self._Dirty.clear()


def RESTRecord(TransRecord):
    def __init__(self,data,uri,connection,read_only=False,dot='_'):
        '''
        data: 初始数据,init data
        uri: 资源识别,Uniform Resource Identifier
        connection: rest链接对象,rest http connection agent

        '''
        super(RESTRecord,self).__init__(data,read_only,dot)
        self._Uri = uri
        self._Connection = connection

    def pull(self):
        '''
        update data
        self._Connection return   data to load in r RESTRecord
            ['__RECORD__',{}]
        '''
        rc = self._Connection.get(self._Uri)
        self.load(rc)

    def push(self):
        '''
        push put data to update the server side data
        update data use the http post method
        if sucess post will return None ,oherwis return error message
        '''
        return self._Connection.post(self._Uri,dict(self._Dirty))

    def commit(self):
        error = self.push()
        if not error: #r['STATUS'] == 200:
            TransRecord.commit(self)


class RecordSet(Meta):
    def __init__(self,data,read_only=False,dot='_'):
        '''
        data:[dict/OderedDict/Record,...]
        
        '''
        super(RecordSet,self).__init__(read_only)
        self._Records = [Record(d,dot=dot) for d in data]
        self._RecordCount = len(self._Records)
        self._CurrentIndex = 0 if self._RecordCount > 0 else None
        self._CurrentRecord = self._Records[0] if self._RecordCount > 0 else Meta(read_only=True)
        
    def __setitem__(self,item,value):
        if self.isReadOnly() or isinstance(item,int):
            return
        self._CurrentRecord[item] = value

    def __getitem__(self,item):
        '''
        item: int return a Record position at item index,str return _CurrentRecord item value
        '''
        if isinstance(item,int):
            return self._Records[item]
        return self._CurrentRecord[item]

    def __getattr__(self,attr):
        '''
        attr: str return _CurrentRecord attr value
        '''
        return getattr(self._CurrentRecord,attr)

    def __len__(self):
        return self._RecordCount

    def __setattr__(self,attr,value):
        if self.isReadOnly():
            return
        self._CurrentRecord[attr] = value

    def first(self):
        if self._RecordCount >0:
            self._CurrentIndex = 0
            self._CurrentRecord = self._Records[self._CurrentIndex]

    def load(self,data):
        '''
        data: format ['__RECORDSET__',fields list/tuple,Recordable list/tuple]
            fields list/tuple, option.字段名称列表或是可以变为Record实例的字典列表
            Recordable,[dict/OderedDict/Record,...],如果使用字段名称列表时,Recordable参数将是字段列表对应的记录内容列表
        '''
        if data[0] != '__RECORDERSET__':
            raise Exception("'__RECORDSET__' must at loading data list/tupe  first item.")
        if isinstance(data[1],(list,tuple)) and isinstance(data[1][0],str):
            #guess the first item is  field
            self._Records = []
            for d in data[2]:
                self._Records.append(TransRecord((data[1],d),dot=self._Dot))
        else:
            self._Records = [TransRecord(d,dot=dot) for d in data[1]]
        self._RecordCount = len(self._Records)
        self._CurrentIndex = 0 if self._RecordCount > 0 else None
        self._CurrentRecord = self._Records[0] if self._RecordCount > 0 else Meta(read_only=True)

        self._Dirty = {}
        
        
    def next(self):
        if self._CurrentIndex >=  self._RecordCount:
            raise StopIteration()
        self._CurrentIndex += 1
        self._CurrentRecord = self._Records[self._CurrentIndex]

    def prev(self):
        if self._CurrentIndex in  (0,None):
            return
        self._CurrentIndex -= 1
        self._CurrentRecord = self._Records[self._CurrentIndex]

    def last(self):
        if self._RecordCount >0:
            self._CurrentIndex = self._RecordCount -1
            self._CurrentRecord = self._Records[self._CurrentIndex]
        
    def keys(self):
        return self._CurrentRecord.keys()

    def values(self):
        return self._CurrentRecord.values()

        
def TransRecordSet(RecordSet):
    def __init__(self,data,read_only=False,dot='_'):
        '''
        data:[dict/OderedDict/Record,...]
        
        '''
        super(TransRecordSet,self).__init__([],read_only)
        self._Records = [TransRecord(d,dot=dot) for d in data]
        self._RecordCount = len(self._Records)
        self._CurrentIndex = 0 if self._RecordCount > 0 else None
        self._CurrentRecord = self._Records[0] if self._RecordCount > 0 else Meta(read_only=True)

        self._Dirty = {}

    def __setitem__(self,item,value):
        if self.isReadOnly() or isinstance(item,int):
            return
        self._Dirty[self._CurrentIndex]=self._CurrentRecord
        self._CurrentRecord[item] = value

    def __setattr__(self,attr,value):
        if self.isReadOnly():
            return
        self._Dirty[self._CurrentIndex]=self._CurrentRecord        
        self._CurrentRecord[attr] = value

    def commit(self):
        for r in self._Dirty.values():
            r.commit()
        self._Dirty.clear()

    def rollback(self):
        for r in self._Dirty.values():
            r.rollback()
        self._Dirty.clear()


def RESTRecordSet(TransRecordSet):
    '''
    可按页次返回查詢到内容
    connection返回内容格式为
    
    '''
    def __init__(self,uri,connection,id_fields=[],cachecount=-1,read_only=False,dot='_'):
        '''
        uri: dataset uri
        connection: rest http connection agent 
        id_fields: table primary key,记录主键
        cachecount: cache record count ,-1 cache all ,0 not cache pull from the server when get record every time,will clear cache record over cachecount*1.5.cachecount <= total cache record <= cachecount*1.5 .
        缓存的记录数量,-1 缓存所有记录,0 不缓存记录,每次都从服务端拉数据.設定cachecount時,cachecount<=緩沖數量<=cachecount*1.5,緩存數量小於cachecount時會從服務端拉數據,但緩存總數量不會超過cachecount*1.5
        '''
        super(RESTRecordSet,self).__init__([],read_only,dot)
        self._NewId = 1
        self._Uri = uri
        self._IDFields = id_fields
        self._Pages=[]
        self._RecordIndex = {}
        self._CacheRange = [None,None]
        self._Connection = connection
        self._CacheCount = cachecount
        
    def pull(self): #,pull_range=[]):
        rng,erng = None,None
        if self._CacheCount == -1:
            r = self._Connection.get(self._Uri)
        else:
            if self._RecordCount ==0:
                rng = [0,self._CacheCount]
            else:
                sindex,eindex = self._CacheRange[0],self._CacheRange[-1]
                #计算当前记录左端需缓冲数量
                sdiff = self._CurrentIndex - self._CacheCount * 1.5
                sdiff = 0 if sdiff < 0 else sdiff
                if sindex < sdiff:
                    del self._Fields[sindex:sdiff]
                    self._CacheRange[0] = sdiff
                else:
                    rng = [sdiff,sindex]
                #計算當前記錄右端緩沖數量
                ediff = self._CurrentIndex + self._CacheCount * 1.5
                if ediff > eindex:
                    erng = [eindex,ediff]
                else:
                    del self._Fields[ediff:eindex]
                    self._CacheRange[1] = ediff
            if rng:
                r = self._Connection.get(self._Uri,{'range':rng})
                if r[0] == '__RECORDSET__':
                    records=[]
                    for d in data[2]:
                        records.append(TransRecord((data[1],d),dot=self._Dot))
                    records.extend(self._Records)
                    self._Records = records
                    self._RecordCount = r[2]['TOTAL']
                    self._CacheRange[0] = r[2]['RANGE'][0]

            if erng:
                r = self._Connection.get(self._Uri,{'range':erng})
                if r[0] == '__RECORDSET__':
                    records=[]
                    for d in data[2]:
                        records.append(TransRecord((data[1],d),dot=self._Dot))
                    self._Records.extend(records)
                    self._RecordCount = r[2]['TOTAL']
                    self._CacheRange[1] = r[2]['RANGE'][1]

    def first(self):
        if self._CacheRange[0] > 0:
            self._CurrentIndex = 0
            if self._CacheCount != -1:
                self.pull()
        self._CurrentRecord = self._Records[self._CurrentIndex]

    def next(self):
        TransRecordSet.next(self)
        if self._CacheCount != -1:
            self.pull()

    def prev(self):
        TransRecordSet.prev(self)
        if self._CacheCount != -1:
            self.pull()

    def last(self):
        if self._CacheRange[1] < (self._RecordCount - 1):
            self._CurrentIndex = self._RecordCount - 1
            if self._CacheCount != -1:
                self.pull()
        self._CurrentRecord = self._Records[self._CurrentIndex]
        
        









