#!/usr/bin/env python
# -*- coding: utf-8 -*-
try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict

class Meta(object):
    def __init__(self,read_only=False):
        self.__dict__['__metadata__']={} #OrderedDict({})
        self.__dict__['_ReadOnly']=read_only

    def __setitem__(self,item,value):
        self.__dict__['__metadata__'][item]=value

    def __getitem__(self,item):
        return self.__dict__['__metadata__'][item]

    def __getattr__(self,attr):
        return self.__dict__['__metadata__'].get(attr)

    def __setattr__(self,attr,value):
        if self.isReadOnly(): #__dict__['__metadata__'].get('ReadOnly') == True:
            return
        self.__dict__['__metadata__'][attr]=value

    def __delitem__(self,attr):
        self.__dict__['__metadata__'].pop(attr)

    def __delattr__(self,attr):
        self.__dict__['__metadata__'].pop(attr)

    def isReadOnly(self):
        return self.__dict__.get('_ReadOnly',False)

    def setReadOnly(self,read_only):
        self.__dict__['_ReadOnly'] = read_only

    def keys(self):
        return self.__dict__['__metadata__'].keys()

    def values(self):
        return self.__dict__['__metadata__'].values()

    def items(self):
        return self.__dict__['__metadata__'].items()

    def update(self,adict):
        if self.isReadOnly(): #__dict__['__metadata__'].get('ReadOnly') == True:
            return
        self.__dict__['__metadata__'].update(adict)

    def clear(self):
        self.__dict__['__metadata__'].clear()
        

class Record(Meta):
    def __init__(self,data,read_only=False,dot='_'):
        '''
        data: dict or OrderedDict or ([keys],[values])
        dot: 字段名称中含点号(.)时,用属性访问会用下横线代替
        '''
        super(Record,self).__init__(read_only)
        d = OrderedDict()
        if isinstance(data,(dict,OrderedDict)):
            d = OrderedDict(data)
        elif isinstance(data,Record):
            d = data.__dict__['__metadata__']
        elif isinstance(data,(list,tuple)) and \
             isinstance(data[0],(list,tuple)) and \
             isinstance(data[1],(list,tuple))  :
            mlen = min(len(data[0]),len(data[1]))
            d = OrderedDict(zip(data[0][:mlen],data[1][:mlen]))

        self.__dict__['__metadata__'] = d
        self.__dict__['_Dot'] = dot
        self.__dict__['_Keys'] = list(d.keys())

    def __setitem__(self,item,value):
        if self.isReadOnly():
            return
        if isinstance(item,int):
            self.__dict__['__metadata__'][self._Keys[item]] = value
        else:
            k = item
            while self._Dot in k and k not in self._Keys:
                k = k.replace(self._Dot,'.',1)
            if k in self._Keys:
                self.__dict__['__metadata__'][item] = value

    def __getitem__(self,item):
        if isinstance(item,int):
            return self.__dict__['__metadata__'][self._Keys[item]]
        return self.__dict__['__metadata__'][item]

    def __getattr__(self,attr):
        k = attr
        dot = self.dot()
        while dot in k and k not in self.keys():
            k = k.replace(dot,'.',1)
        return self.__dict__['__metadata__'].get(k)

    def __len__(self):
        return len(self._Keys)

    def __setattr__(self,attr,value):
        if self.isReadOnly():
            return 
        k = attr
        dot = self.dot()
        keys = self.keys()
        while dot in k and k not in keys:
            k = k.replace(dot,'.',1)
        if k in keys:
            self.__dict__['__metadata__'][k]=value
            
    def asdict(self):
        return dict(self.__dict__['__metadata__'])

    def dot(self):
        return self.__dict__['_Dot']

    def keys(self):
        return self.__dict__['_Keys']

    def values(self):
        return self.__dict__['__metadata__'].values()

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
            d = data[1].__dict__['__metadata__']
        elif isinstance(data[1],(list,tuple)) and \
             isinstance(data[1][0],(list,tuple)) and \
             isinstance(data[1][1],(list,tuple))  :
            mlen = min(len(data[1][0]),len(data[1][1]))
            d = OrderedDict(zip(data[1][0][:mlen],data[1][1][:mlen]))
        self.__dict__['__metadata__'] = d
        self._Keys = list(d.keys())
        

class TransRecord(Record):
    def __init__(self,data,key_fields=[],read_only=False,dot='_'):
        super(TransRecord,self).__init__(data,read_only)
        self.__dict__['_KeyFields'] = key_fields
        self.__dict__['_Dirty'] = Meta()

    def __setattr__(self,attr,value):
        if self.isReadOnly():
            return 
        k = attr
        while self._Dot in k and k not in self._Keys:
            k = k.replace(self._Dot,'.',1)
        if k in self._Keys:
            self._Dirty[k] = self.__dict__['__metadata__'][k]
            self.__dict__['__metadata__'][k]=value
        
    def __setitem__(self,item,value):
        if self.isReadOnly():
            return
        if isinstance(item,int):
            self.__dict__['__metadata__'][self._Keys[item]] = value
        else:
            k = item
            while self._Dot in k and k not in self._Keys:
                k = k.replace(self._Dot,'.',1)
            if k in self._Keys:
                self._Dirty[k] = self.__dict__['__metadata__'][item]
                self.__dict__['__metadata__'][item] = value

    def rollback(self):
        for k,v in self._Dirty.items():
            self.__dict__['__metadata__'][k] = v

    def commit(self):
        print('transrecord commit',self.__dict__['_Dirty'])
        self.__dict__['_Dirty'].clear()


class RESTDataSource(Meta):
    '''
    HttpClient
    提供http rest接口,一个HttpClient实例可对应多个RESTDataSource
        RESTDataSource._Uri與RESTDataSet._Uri組成完成的HttpClient請求Uri
    RESTDataSource
    为RESTDataSet与HttpClient提供嫁接功能,简化RESTDataSet操作
    
    RESTDataSet
    对指定http uri数据提供类数据表的用户层操作接口
    '''
    def __init__(self,httpclient,uri='/',read_only=False,dot='_'):
        super(RESTDataSource,self).__init__(read_only)
        self.__dict__['_Uri'] = uri
        self.__dict__['_HttpClient'] = httpclient
        self.__dict__['_LastError'] = None
        self.__dict__['_Dot'] = dot
        
    def get(self,restdataset,*item,paras={}):
        '''
        get a restrecord item
        restdataset: RESTDataSet instance
        item: RESTDataSet keyfield values
        paras: url query parameters
        '''
        uri ='%s/%s' % (self._Uri, restdataset.uri(*item)) #dataset.getUri(*key_values))
        r = self._HttpClient.get(uri,paras)
        if r['STATUS'] == 200:
            rec = r['DATA']
        else:
            self.__dict__['_LastError'] =  '%(STATUS)s:%(MESS)s.' % r
            return
        if r[0] == '__RECORD__':
            r = RESTRecord(r[1:],restdataset,self._Dot)
        else:
            self.__dict__['_LastError'] = 'data format error'
            r = None
        return r

    def uri(self):
        return self._Uri

    def getError(self):
        return self._LastError

    def pull(self,dataset,pull_range=[],filter=[]):
        '''
        dataset: RESTDataSet instance
        range: return query range
        filter: query condition
        '''
        #uri = '%s/%s' % (self._Uri,table.replace('.','/'))
        uri = '%s/%s' % (self._Uri,dataset.uri())
        r = self._HttpClient.get(uri,{'range':pull_range,'filter':filter})
        print('pull',r)
        if r['STATUS'] == 200:
            rec = r['DATA']
        else:
            self.__dict__['_LastError'] =  '%(STATUS)s:%(MESS)s.' % r
            return
        if rec[0] == '__DATASET__':
            rec.append({})
            flag,fields,rawrec,extpara = rec[:4]
            r = [RESTRecord((fields,d),self,self._Dot) for d in rawrec]
            r.append(extpara)
        else:
            self.__dict__['_LastError'] = 'data format error'
            r = None
        print('return ',r)
        return r

    def post(self,uri,paras={}):
        '''
        update record
        '''
        return self._HttpClient.post(uri,paras)

    def put(self,uri,paras={}):
        '''
        append record
        '''
        return self._HttpClient.put(uri,paras)

    def delete(self,uri,paras={}):
        return self._HttpClient.delete(uri,paras)

    def search(self,filters={}):
        pass

class RESTRecord(TransRecord):
    def __init__(self,data,restrecordset,read_only=False,dot='_'):
        '''
        data:[(fields),(values) 或 OrderredDict()]
        restdataset:RESTDataSet instance
        '''
        super(RESTRecord,self).__init__(data,read_only,dot)
        self.__dict__['_RestDataSet'] = restdataset
        self.__dict__['_KeyValues'] = [self[kf] for kf in self.keyFields()]

    def rawData(self):
        return self.__dict__['__metadata__']

    def dataSource(self):
        return self._RestDataSet.getDataSource()

    def dataset(self):
        return self._RestDataSet

    def keyFields(self):
        return self._RestDataSet.keyFields()
        
    def uri(self):
        return '%s/%s' % (self.dataSource.uri(),self.dataset().uri(*tuple(self._KeyValues)))

    def pull(self):
        '''
        pull data from remote host
        '''
        rc = self.dataSource().get(self.dataset(),*tuple(self._KeyValues))
        if rc:
            self.rollback()
            self.__dict__['__metadata__'].update(rc.rawData())
        else:
            return self.dataSource().getError()

    def push(self):
        '''
        push put data to update the server side data
        update data use the http post method
        if sucess post will return None ,oherwis return error message
        '''
        return self._DataSource.post(self._TableName,self._KeyValues,dict(self._Dirty))

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
        #self.__dict__['_ReadOnly']=read_only
        self.__dict__['_Records'] = [Record(d,dot=dot) for d in data]
        self.__dict__['_RecordCount'] = len(self._Records)
        self.__dict__['_CurrentIndex'] = 0 if self._RecordCount > 0 else None
        self.__dict__['_CurrentRecord'] = self._Records[0] if self._RecordCount > 0 else Meta(read_only=True)

        
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
        return self.__dict__['_CurrentRecord'][attr]

    def __len__(self):
        return self._RecordCount

    def __setattr__(self,attr,value):
        print('recordset readonly', self.isReadOnly())
        if self.isReadOnly():
            return
        self._CurrentRecord[attr] = value

    def first(self):
        if self._RecordCount >0:
            self.__dict__['_CurrentIndex'] = 0
            self.__dict__['_CurrentRecord'] = self._Records[self._CurrentIndex]


    def load(self,data):
        '''
        data: format ['__DATASET__',fields list/tuple,Recordable list/tuple]
            fields list/tuple, option.字段名称列表或是可以变为Record实例的字典列表
            Recordable,[dict/OderedDict/Record,...],如果使用字段名称列表时,Recordable参数将是字段列表对应的记录内容列表
        '''
        if data[0] != '__DATASET__':
            raise Exception("'__DATASET__' must at loading data list/tupe  first item.")
        if isinstance(data[1],(list,tuple)) and isinstance(data[1][0],str):
            #guess the first item is  field
            rec = []
            for d in data[2]:
                rec.append(TransRecord((data[1],d),dot=self._Dot))
        else:
            rec = [TransRecord(d,dot=dot) for d in data[1]]
        self.__dict__['_Records'] = rec 
        self.__dict__['_RecordCount'] = len(self._Records)
        self.__dict__['_CurrentIndex'] = 0 if self._RecordCount > 0 else None
        self.__dict__['_CurrentRecord'] = self._Records[0] if self._RecordCount > 0 else Meta(read_only=True)
        self.__dict__['_Dirty'] = {}
        
    def Next(self):
        if self._CurrentIndex >=  self._RecordCount:
            return
        self.__dict__['_CurrentIndex'] =  self._CurrentIndex + 1        
        self.__dict__['_CurrentRecord'] = self._Records[self._CurrentIndex]
  
        
    def next(self):
        if self._CurrentIndex >=  self._RecordCount:
            raise StopIteration()
        self._CurrentIndex += 1
        self._CurrentRecord = self._Records[self._CurrentIndex]      

    def prev(self):
        if self._CurrentIndex in  (0,None):
            return
        self.__dict__['_CurrentIndex'] =  self._CurrentIndex - 1        
        self.__dict__['_CurrentRecord'] = self._Records[self._CurrentIndex]

    def last(self):
        if self._RecordCount >0:
            self.__dict__['_CurrentIndex'] =  self._RecordCount -1
            self.__dict__['_CurrentRecord'] = self._Records[self._CurrentIndex]
        
    def keys(self):
        return self._CurrentRecord.keys()

    def values(self):
        return self._CurrentRecord.values()

        
class TransRecordSet(RecordSet):
    def __init__(self,key_fields=[],data=[],read_only=False,dot='_'):
        '''
        key_fields: 主键名称列表
        data:[dict/OderedDict/Record,...]
        
        '''
        super(TransRecordSet,self).__init__(data,read_only)
        ## self.__dict__['_TableName'] = table_name
        ## self.__dict__['_RESTDataSource'] = restdatasource
        self.__dict__['_Records'] = [TransRecord(d,dot=dot) for d in data]
        self.__dict__['_RecordCount'] = len(self._Records)
        self.__dict__['_CurrentIndex'] = 0 if self._RecordCount > 0 else None
        self.__dict__['_CurrentRecord'] = self._Records[0] if self._RecordCount > 0 else Meta(read_only=True)

        self.__dict__['_Dirty'] = {}

    def __setitem__(self,item,value):
        if self.isReadOnly() or isinstance(item,int):
            return
        self.__dict__['_Dirty'][self._CurrentIndex]=self._CurrentRecord
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


class RESTRecordSet(TransRecordSet):
    def __init__(self,table_name,restdatasource,key_fields=[],cachecount=-1,read_only=False,dot='_'):
        '''
        table_name:table_name: rest data uri path '/' replace by '.'  ,rest数据访问路径,但'/'被'.'代替
        restdatasource: rest http connection agent 
        key_fields: table primary key,记录主键
        cachecount: cache record count ,-1 cache all ,0 not cache pull from the server when get record every time,will clear cache record over cachecount*1.5.cachecount <= total cache record <= cachecount*1.5 .
        缓存的记录数量,-1 缓存所有记录,0 不缓存记录,每次都从服务端拉数据.設定cachecount時,cachecount<=緩沖數量<=cachecount*1.5,緩存數量小於cachecount時會從服務端拉數據,但緩存總數量不會超過cachecount*1.5
        '''
        super(RESTRecordSet,self).__init__(key_fields,[],read_only,dot)
        self.__dict__['_NewId'] = 1
        self.__dict__['_KeyFields'] = key_fields
        self.__dict__['_TableName'] = table_name
        self.__dict__['_RecordIndex'] = {}
        self.__dict__['_CacheRange'] = [None,None]
        self.__dict__['_DataSource'] = restdatasource
        self.__dict__['_CacheCount'] = cachecount
        self.__dict__['_Filter']=[]


    def keyFields(self):
        return self._KeyFields

    def dataSource(self):
        return self._DataSource

    def uri(self,*key_values):
        return '%s/%s' % (self._TableName.replace('.','/'),'/'.join(key_values))

    ## def insert(self,index,records):
    ##     '''
    ##     index:  position to insert data
    ##     records: RESTRecordSet instance or __DATADSET__ list
        
    ##     '''

    def refresh(self):
        '''
        pull all the cache records from the host

        '''

    def setFilter(self,filter):
        self.__dict__['_Filter'] = filter

    def getFilter(self):
        return self.__dict__['_Filter']

    def search(self,filters):
        self.setFilter(filters)
        self.pull(True)

    def pull(self,clear=False): #uri=None,clear=False): #,pull_range=[]):
        '''
        返回非None,表示错误信息
        '''
        error = ''
        if clear:
            self.__dict__['_RecordIndex'].clear() # = {}
            self.__dict__['_CacheRange'] = [None,None]
            self.__dict__['_Records'].clear()
            self.__dict__['_CurrentIndex'] = None
            
        rng,erng = None,None
        if self._CacheCount == -1:
            if self._CacheRange[0] is None:
                rng = [-1,-1]
        else:
            if self._RecordCount ==0:
                rng = [0,self._CacheCount]
            else:
                sindex , eindex = self._CacheRange[0] or 0 , self._CacheRange[-1] or 0
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
            rs = self._DataSource.pull(self,rng,self._Filter)
            if rs:
                self.__dict__['_Records'][0:0] = rs[:-1]
                self.__dict__['_RecordCount'] = rs[-1].get('TOTAL',0)
                self.__dict__['_CacheRange'][0] = rs[-1].get('RANGE',0)
            else:
                error = self._DataSource.getError()
        if erng and not error:
            rs = self._DataSource.pull(self,erng,self._Filter)
            if rs:
                self.__dict__['_Records'].extend(rs[:-1])
                self.__dict__['_RecordCount'] = rs[-1].get('TOTAL',0)
                self.__dict__['_CacheRange'][0] = rs[-1].get('RANGE',0)
            else:
                error = self._DataSource.getError()
        return error

    def __getitem__(self,item):
        '''
        item: int return a Record position at item index,str return _CurrentRecord item value
        '''
        if isinstance(item,int):
            return self._Records[item]
        return self._CurrentRecord[item]

    def get(self,*item):
        '''
        item: get an item by primary key.item对应KeyFIeld指定字段名称
        return item data
        '''
        #kv = 
        rec = self._RecordIndex.get(self._Dot.join(item))
        if rec:
            pass
        else:
            uri = self.uri(*item)
            
        es = self._Connection.get(uri)
        if rs[0] == '__RECORDSET__':
            return TransRecord(rs[1],rs[2][0])
        else:
            return rs
        
    
    def first(self):
        if self._CacheRange[0] > 0:
            self._CurrentIndex = 0
            if self._CacheCount != -1:
                self.pull()
        self._CurrentRecord = self._Records[self._CurrentIndex]

    def Next(self):
        TransRecordSet.Next(self)
        if self._CacheCount != -1:
            return self.pull()

    def next(self):
        TransRecordSet.next(self)
        if self._CacheCount != -1:
            self.pull()

    def prev(self):
        TransRecordSet.prev(self)
        if self._CacheCount != -1:
            return self.pull()

    def last(self):
        er = None
        if self._CacheRange[1] < (self._RecordCount - 1):
            self._CurrentIndex = self._RecordCount - 1
            if self._CacheCount != -1:
                er = self.pull()
        self._CurrentRecord = self._Records[self._CurrentIndex]
        return er
        
    def search(self,filters={}):
        self.__dict__['_Filter']=filters
        return self.pull(True)

    def setUri(self,uri):
        self.__dict__['_Uri'] = uri

