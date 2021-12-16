import grpc
import FggDataService_pb2
import FggDataService_pb2_grpc
from enum_types import *
from GraphItem import *

""" represents type system for the CBOSS API"""
class FeatureData:
    """ Base class for xmit and receive"""

    def __init__(self, attrkey = 0):
        self.attrkey = attrkey
        self.last = None
        self.keyseq = []
        self.date2value = {}

    def __str__(self):
        return str(self.attrkey) + ':' + str(self.keyseq) + ':' + str(self.date2value)

    def setKey(self, key):
        self.keyseq.append(key)

    def getScdDates(self, set):
        for key in sorted(self.date2value):
            set.add(self.date2value[key])

    def getValue(self, asof):
        if len(self.date2value) > 100:
            print('slow. chg to bsearch')
        val = None
        for key in sorted(self.date2value):
            if key <= asof:
                val = self.date2value[key]
            else:
                break
        return val

    def setValue(self, dt, val):
        for key in sorted(self.date2value):
            if dt >= key and self.date2value[key] == val:
                return
        self.date2value[dt] == val

    def reset(self, bPreserveKeys):
        self.attrkey = 0
        self.date2value = {}
        if bPreserveKeys: return
        self.keyseq = []

    def xmits(self):
        results = []
        results.append(FggDataService_pb2.FggData(field=XMIT.KEYSEQ,int_value=self.keyseq[0]))
        attr = GraphItem.findByPK(self.attrkey)

        dtype = None
        if isinstance(attr, NodeAttr) or isinstance(attr, EdgeAttr):
            dtype = getattr(attr,'dtypeEnum')
        else:
            return results

        lastval = None
        for key in sorted(self.date2value):
            if self.date2value[key] != lastval:
                results.append(FggDataService_pb2.FggData(field=XMIT.VALUEDT,int_value=key))
                if dtype == DataType.CHAR or dtype == DataType.STRING or dtype == DataType.ENUM:
                    results.append(FggDataService_pb2.FggData(field=XMIT.VALUE,str_value=self.date2value[key]))
                elif dtype == DataType.LONG:
                    results.append(FggDataService_pb2.FggData(field=XMIT.VALUE,long_value=self.date2value[key]))
                elif dtype == DataType.LONG:
                    results.append(FggDataService_pb2.FggData(field=XMIT.VALUE,dbl_value=self.date2value[key]))
                else:
                    results.append(FggDataService_pb2.FggData(field=XMIT.VALUE,int_value=self.date2value[key]))
                lastval = self.date2value[key]
        results.append(FggDataService_pb2.FggData(field=XMIT.ATTRKEY,int_value=self.attrkey))
        return results

    def add(self, data):
        if self.last == None:
            if data.field == XMIT.VALUEDT:
                self.last = data
            elif data.field == XMIT.KEYSEQ:
                self.keyseq.append(data.int_value);
            elif data.field == XMIT.ATTRKEY:
                self.attrkey = data.int_value
                return True
        else:
            lastval = self.last.int_value
            self.last = None
            if data.field == XMIT.KEYSEQ:
                self.keyseq.append(data.int_value)
            elif data.field == XMIT.VALUE:
                if data.HasField('int_value'):
                    self.date2value[lastval] = data.int_value
                elif data.HasField('long_value'):
                    self.date2value[lastval] = data.long_value
                elif data.HasField('dbl_value'):
                    self.date2value[lastval] = data.dbl_value
                elif data.HasField('str_value'):
                    self.date2value[lastval] = data.str_value
                else:
                    self.date2value[lastval] = None
            elif data.field == XMIT.VALUEDT:
                self.date2value[lastval] = None
                self.last = data
            elif data.field == XMIT.ATTRKEY:
                self.date2value[lastval] = None
                self.attrkey = data.int_value
                return True
        return False
