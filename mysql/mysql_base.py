#encoding=utf-8
"""
封装mysql基本操作的基类
"""

import MySQLdb
import json
import urllib
import urllib2
import time
import array
import types
import re
import datetime
import exceptions
import decimal
import inspect

def _line_number():
	return inspect.currentframe().f_back.f_lineno

def mysql_encode(s):
	return MySQLdb.escape_string(s)

class MysqlBase:
	def __init__(self, config, slave_config=None):
		self.m_mysql = None
		self.m_config = config
		self.m_slave = None
		self.m_slave_config = None
		#if None!=slave_config:
		self.m_slave_config = slave_config

	def __del__(self):
		if self.m_mysql is not None:
			self.m_mysql.close()
			del self.m_mysql
			self.m_mysql = None
		if self.m_slave is not None:
			self.m_slave.close()
			del self.m_slave
			self.m_slave = None

	"""
	连接数据库
	"""
	def _connect_db(self):
		try:
			self.m_mysql = MySQLdb.connect(host=self.m_config['host'], \
									user=self.m_config['user'], \
									passwd=self.m_config['passwd'], \
									db=self.m_config['db'],
									charset=self.m_config.get('charset', 'utf8'),
									port=self.m_config.get('port', 3306),
									connect_timeout = self.m_config.get('connect_timeout',10)
									)
			self.m_mysql.autocommit(True)
		except Exception,e:
			return (False, e)
		if self.m_slave_config is not None:
			try:
				self.m_slave = MySQLdb.connect(host=self.m_slave_config['host'], \
										user=self.m_slave_config['user'], \
										passwd=self.m_slave_config['passwd'], \
										db=self.m_slave_config['db'],
										port=self.m_config.get('port', 3306),
										charset=self.m_config.get('charset', 'utf8'),
										connect_timeout = self.m_config.get('connect_timeout',10)
										)
			except Exception,e:
				return (False, e)
		return (True,None)

	def _query(self, sql, param):
		while True:
			if self.m_mysql is None:
				ret = self._connect_db()
				if not ret[0]:
					print __file__, _line_number(), 'connect db error:%s' % str(ret[1])
					raise ret[1]
			mysql = self.m_mysql if self.m_slave is None else self.m_slave
			cursor = mysql.cursor()
			try:
				cursor.execute(sql, param)
				break
			except MySQLdb.Error,e:
				error_code = e.args[0]
				if 2006==error_code:  #数据库连接中断
					self.m_mysql = None
					self.m_slave = None
					continue
				else:
					print __file__, _line_number(), 'execute sql error:%s' % str(e)
					raise e
			except Exception,e1:
				print __file__, _line_number(), 'execute sql error:%s' % str(e1)
				raise e1
		#end while
		return cursor.fetchall()

	def _paging_query(self, query_sql, param1, count_sql, param2):
		dataset = self._query(query_sql, param1)
		#change to array
		arr = []
		for row in dataset:
			arr_row = []
			for col in row:
				arr_row.append(col)
			arr.append(arr_row)
		dataset = self._query(count_sql, param2)
		total = dataset[0][0]
		m = {'total':total, 'root':arr}
		return m

	def _paging_query_with_meta(self, query_sql, count_sql, count_param):
		dataset = self._query_with_meta(query_sql)
		dataset_1 = self._query(count_sql, count_param)
		dataset['total'] = dataset_1[0][0]
		return dataset

	def _query_to_tsv(self, sql, param):
		dataset = self._query(sql, param)
		arr = array.array('c')
		for row in dataset:
			for col in row:
				col_type = type(col)
				if col_type is types.IntType or \
				   col_type is types.BooleanType or \
				   col_type is types.LongType or \
				   col_type is types.FloatType:
					arr.fromstring(str(col))
					arr.fromstring('\t')
				elif isinstance(col, decimal.Decimal):
					arr.fromstring(str(long(col)))
					arr.fromstring('\t')
				elif col_type is types.StringType:
					arr.fromstring('\"')
					arr.fromstring(col)
					arr.fromstring('\"\t')
				else:
					arr.fromstring('\"')
					arr.fromstring(str(col))
					arr.fromstring('\"\t')
			arr.fromstring('\n')
		#
		return arr.tostring()

	def _query_with_meta(self, sql):
		while True:
			if self.m_mysql is None:
				ret = self._connect_db()
				if not ret[0]:
					print __file__, _line_number(), 'connect db error:%s' % str(ret[1])
					raise ret[1]
			mysql = self.m_mysql if self.m_slave is None else self.m_slave
			#cursor=self.m_mysql.cursor()
			try:
				mysql.query(sql)
				break
			except MySQLdb.Error,e:
				error_code = e.args[0]
				if 2006==error_code:  #数据库连接中断
					self.m_mysql = None
					self.m_slave = None
					continue
				else:
					print __file__, _line_number(), 'execute sql error:%s' % str(e)
					raise e
			except Exception,e1:
				print __file__, _line_number(), 'execute sql error:%s' % str(e1)
				raise e1
		#end while
		r = mysql.store_result()
		out_map = {'total':r.num_rows(), 'field_count':r.num_fields()}
		fields = []
		(field_info) = r.describe()
		"""
		enum enum_field_types { MYSQL_TYPE_DECIMAL=0, MYSQL_TYPE_TINY,
					MYSQL_TYPE_SHORT,  MYSQL_TYPE_LONG,
					MYSQL_TYPE_FLOAT,  MYSQL_TYPE_DOUBLE=5,
					MYSQL_TYPE_NULL,   MYSQL_TYPE_TIMESTAMP=7,
					MYSQL_TYPE_LONGLONG=8,MYSQL_TYPE_INT24=9,
					MYSQL_TYPE_DATE=10,   MYSQL_TYPE_TIME=11,
					MYSQL_TYPE_DATETIME=12, MYSQL_TYPE_YEAR,
					MYSQL_TYPE_NEWDATE=14, MYSQL_TYPE_VARCHAR=15,
					MYSQL_TYPE_BIT,
								MYSQL_TYPE_NEWDECIMAL=246,
					MYSQL_TYPE_ENUM=247,
					MYSQL_TYPE_SET=248,
					MYSQL_TYPE_TINY_BLOB=249,
					MYSQL_TYPE_MEDIUM_BLOB=250,
					MYSQL_TYPE_LONG_BLOB=251,
					MYSQL_TYPE_BLOB=252,
					MYSQL_TYPE_VAR_STRING=253,
					MYSQL_TYPE_STRING=254,
					MYSQL_TYPE_GEOMETRY=255

		};
		"""
		for item in field_info:
			temp_dict = {'name':item[0]}
			#str_data_type = ''
			if item[1]==0 or \
			   item[1]==1 or \
			   item[1]==2 or \
			   item[1]==3 or \
			   item[1]==4 or \
			   item[1]==5 or \
			   item[1]==8 or \
			   item[1]==9 or \
			   item[1]==246:
				temp_dict['type'] = 'number'
			elif item[1]==7 or \
				item[1]==12:
				temp_dict['type'] = 'date'
				temp_dict['dateFormat'] = 'Y-n-j G:i:s'
			elif item[1]==10 or item[1]==14:
				temp_dict['type'] = 'date'
				temp_dict['dateFormat'] = 'Y-n-j'
			elif item[1]==12:
				temp_dict['type'] = 'date'
				temp_dict['dateFormat'] = 'G:i:s'
			elif item[1]==15 or item[1]==253 or item[1]==254:
				temp_dict['type'] = 'string'
			elif item[1]==252:  #blob
				temp_dict['type'] = 'string'
			else:
				return 'unknown mysql type:%d' % item[1]
			fields.append(temp_dict)
		out_map['fields'] = fields
		#print str(out_map)
		#return
		datas = []
		for i in range(r.num_rows()):
			(row,) = r.fetch_row()
			#print str(row)
			#return;
			new_row = []
			for col in row:
				if col is None:
					new_row.append('')
				elif type(col)==types.FloatType or \
					type(col)==types.IntType or \
					type(col)==types.LongType:
					new_row.append(col)
				elif type(col)==types.StringType:
					new_row.append(col)
				elif type(col)==types.UnicodeType:
					new_row.append(col)
					#print 'string types:%s' % col
				elif isinstance(col, datetime.datetime):
					new_row.append(col.strftime('%Y-%m-%d %H:%M:%S'))
				elif isinstance(col, datetime.date):
					new_row.append(col.strftime('%Y-%m-%d'))
				#elif isinstance(col, time.time):
				#	new_row.append(col.strftime('%H:%M:%S'))
				else:
					raise exceptions.TypeError,'unknown type:%s,value:%s,doc:%s,t=%s' \
							% (dir(type(col)), str(col), col.__doc__, str(col.__class__))
			datas.append(new_row)
		out_map['root'] = datas
		return out_map

	def _execute(self, sql, param):
		if self.m_config.get('read_only', False):
			return -1
		while True:
			if self.m_mysql is None:
				ret = self._connect_db()
				if not ret[0]:
					print __file__, _line_number(), 'connect db error:%s' % str(ret[1])
					raise ret[1]
			cursor=self.m_mysql.cursor()
			#转换参数
			#print str(param)
			#param_len = len(param)
			#for index in range(param_len):
			#	item = param[index]
			#	if type(item)==types.StringType or \
			#		type(item)==types.UnicodeType:
			#		param[index] = self.m_mysql.escape_string(item)
			#print str(param)
			try:
				ret = cursor.execute(sql, param)
				self.m_mysql.commit()
				return ret
			except MySQLdb.Error,e:
				error_code = e.args[0]
				if 2006==error_code:  #数据库连接中断
					self.m_mysql = None
					continue
				else:
					print __file__, _line_number(), 'execute sql error:%s' % str(e)
					print __file__, _line_number(), sql
					print __file__, _line_number(), str(param)
					raise e
			except Exception,e1:
				print __file__, _line_number(), 'execute sql error:%s' % str(e1)
				print __file__, _line_number(), sql
				print __file__, _line_number(), str(param)
				raise e1
		#end while

	'''
	检查一个表是否存在
	'''
	def _table_exists(self, db_name, table_name):
		sql = 'select count(1) from information_schema.TABLES \
			where TABLE_SCHEMA=%s and TABLE_NAME=%s'
		dataset = self._query(sql, [db_name, table_name])
		return True if dataset[0][0]!=0 else False
