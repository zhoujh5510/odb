#!/usr/bin/env python
#-*- coding: UTF-8 -*- 

'''
File Name: 
	 odb_performance_test.py
Description: 
	 It test load performance of odb untility.
Author: 
	 pengpeng.yang
modified by:
        pengpeng.yang
 FileCreatedTime: 
	 2017.09.15
 LastModifiedTime: 
	 2017.09.18
'''

import commands
import logging
import time
import os
import random
import sys, getopt


max_to_test_performance = 10000000

preCmd = ""
dbTraf=""
usernameTraf=""
passwdTraf=""

#args , getopt
def opts_args_parse():
	opts, args = getopt.getopt(sys.argv[1:], "hi:j:k:l:", ["preCmd=", "dbTraf=", "usernameTraf=", "passwdTraf="])
        print  len(opts)
        print args
        if len(opts) != 4:
                usage()
                sys.exit()
        for op, value in opts:
                if op == "-i":
                        global preCmd
                        preCmd = value
                elif op == "-j":
                        global dbTraf
                        dbTraf = value
                elif op == "-k":
                        global usernameTraf
                        usernameTraf = value
                elif op == "-l":
                        global passwdTraf
                        passwdTraf = value
                elif op == "-h":
                        usage()
                        sys.exit()
                elif op == "-help":
                        usage()
                        sys.exit()


def usage():
        print "usage short options example:"
        print "python odb_performance_test.py -i odb64luo -j traf -k trafodion -l traf123"
        print "i - -preCmd is location for odb64luo "
        print "j - -dbTraf is config ablout trafodion in odbc.ini"
        print "k - -usernameTraf is login username for trafodion"
        print "l - -passwdTraf is login passwd for trafodion"

opts_args_parse()


print
print "now prepare files/scripts/tables for odb test ..."
print 
# create logger
logger_sumary = logging.getLogger('sumary_log')
logger_sumary.setLevel(logging.DEBUG)
logger_detail = logging.getLogger('sumary_detail')
logger_detail.setLevel(logging.DEBUG)

#create log folder
folder = 'odb_performance_log'
if os.path.exists(folder):
	if os.path.isfile(folder):
		os.remove(folder)
		os.mkdir(folder)
else:
	os.mkdir(folder)

# create handler,to write log file
ISOTIMEFORMAT = '%Y-%m-%d-%H-%M-%S'
log_path_sum = folder + '/odb_log_sumary_' + str(time.strftime(ISOTIMEFORMAT))
sum_handle = logging.FileHandler(log_path_sum)
sum_handle.setLevel(logging.DEBUG)
log_path_detail = folder + '/odb_log_detail_' + str(time.strftime(ISOTIMEFORMAT))
detail_handle = logging.FileHandler(log_path_detail)
detail_handle.setLevel(logging.DEBUG)
print "now create log file"
print "sumary log file :" + log_path_sum
print "detail log file :" + log_path_detail
print 

# create handler,to write consle
consle_handle = logging.StreamHandler()
consle_handle.setLevel(logging.DEBUG)

# define handler format
# %(asctime)s - %(name)s - %(levelname)s - 
formatter = logging.Formatter('%(message)s')
sum_handle.setFormatter(formatter)
consle_handle.setFormatter(formatter)
detail_handle.setFormatter(formatter)

# add handler to logger
logger_sumary.addHandler(sum_handle)
logger_detail.addHandler(consle_handle)
logger_detail.addHandler(detail_handle)	

def create_output():
	print "now create output folder"
	print "please wait..."

	output_folder = 'performance_output_data'
	if os.path.exists(output_folder):
		if os.path.isfile(output_folder):
			os.remove(output_folder)
			os.mkdir(output_folder)
	else:
		os.mkdir(output_folder)
	
	
def execute_cmd(num, usecase, cmd):
	session_start = 0
	session_end = 0
	session_start = time.time()
	(status, output) = commands.getstatusoutput(cmd)
	session_end = time.time()
	logger_sumary.info('------------------ test : %d ------------------', num)
	logger_detail.info('------------------ test : %d ------------------', num)
	logger_sumary.info('Usecase : %s', usecase)
	logger_detail.info('Usecase : %s', usecase)
	logger_sumary.info('Command : %s', cmd)
	logger_detail.info('Command : %s', cmd)

	if status == 0:
		if output.find('Error') != -1:
			status = -1
			logger_sumary.info('Result : failed')
			logger_detail.info('Result : failed')
		else:
			logger_sumary.info('Result : success')
			logger_detail.info('Result : success')
	else:
		logger_sumary.info('Result : failed')
		logger_detail.info('Result : failed')
	
	logger_sumary.info('During : %ds\n', session_end - session_start)
	logger_detail.info('During : %ds', session_end - session_start)
	
	logger_detail.info('Details :\n%s\n', output)
	print
	return status

def main():
	global max_to_test_performance
	
	total_num = 0
	failed_num = 0
	ret = 0
	start_time = 0
	end_time = 0
	
	start_time = time.time()

	global preCmd
	global dbTraf
	global usernameTraf
	global passwdTraf
	database=dbTraf
	username=usernameTraf
	passwd=passwdTraf
	
	#test 1
	total_num += 1
	cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf + ' -l src=odb_performance_loadfile/load_data_performance:pre=@performance_scripts/ddl_big_table1.sql:tgt=trafodion.odb_performance_test.big_table1:max=' + str(max_to_test_performance) + ':rows=5000:parallel=6:loadcmd=IN:fs=\|:sq=\\" -v'
	usecase = 'Performance with loadcmd IN(insert)'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 2
	total_num += 1
	cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf + ' -l src=odb_performance_loadfile/load_data_performance:pre=@performance_scripts/ddl_big_table2.sql:tgt=trafodion.odb_performance_test.big_table2:max=' + str(max_to_test_performance) + ':rows=5000:parallel=6:loadcmd=UP:fs=\|:sq=\\" -v'
	usecase = 'Performance with loadcmd UP(upsert)'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 3
	total_num += 1
	cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf + ' -l src=odb_performance_loadfile/load_data_performance:pre=@performance_scripts/ddl_big_table3.sql:tgt=trafodion.odb_performance_test.big_table3:max=' + str(max_to_test_performance) + ':rows=5000:parallel=6:loadcmd=UL:fs=\|:sq=\\" -v'
	usecase = 'Performance with loadcmd UL(upsert using load)'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1		
	
	#test 4
	total_num += 1
	cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf + ' -l src=odb_performance_loadfile/load_data_performance:pre=@performance_scripts/ddl_big_table4.sql:tgt=trafodion.odb_performance_test.big_table4:max=' + str(max_to_test_performance) + ':rows=5000:parallel=2:loadcmd=UL:fs=\|:sq=\\" -v'
	usecase = 'Performance of loading with 2 parallel threads'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1
	
	#test 5
	total_num += 1
	cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf + ' -l src=odb_performance_loadfile/load_data_performance:pre=@performance_scripts/ddl_big_table5.sql:tgt=trafodion.odb_performance_test.big_table5:max=' + str(max_to_test_performance) + ':rows=5000:parallel=4:loadcmd=UL:fs=\|:sq=\\" -v'
	usecase = 'Performance of loading with 4 parallel threads'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 6
	total_num += 1
	cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf + ' -l src=odb_performance_loadfile/load_data_performance:pre=@performance_scripts/ddl_big_table6.sql:tgt=trafodion.odb_performance_test.big_table6:max=' + str(max_to_test_performance) + ':rows=5000:parallel=100:loadcmd=UL:fs=\|:sq=\\" -v'
	usecase = 'Performance of loading with parallel threads more than avalible mxosrvrs'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1
	
	#test 7
	total_num += 1
	cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf + ' -l src=odb_performance_loadfile/load_data_performance:pre=@performance_scripts/ddl_big_table7.sql:tgt=trafodion.odb_performance_test.big_table7:max=' + str(max_to_test_performance) + ':rows=1000:parallel=6:loadcmd=UL:fs=\|:sq=\\" -v'
	usecase = 'Performance using 1000 as rowset'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 8
	total_num += 1
	cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf + ' -l src=odb_performance_loadfile/load_data_performance:pre=@performance_scripts/ddl_big_table8.sql:tgt=trafodion.odb_performance_test.big_table8:max=' + str(max_to_test_performance) + ':rows=5000:parallel=6:loadcmd=UL:fs=\|:sq=\\" -v'
	usecase = 'Performance using 5000 as rowset'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 9
	total_num += 1
	cmd = preCmd + ' -u ' +  usernameTraf + ' -p ' + passwdTraf + ' -d ' + dbTraf + ' -l src=odb_performance_loadfile/load_data_performance:pre=@performance_scripts/ddl_big_table9.sql:tgt=trafodion.odb_performance_test.big_table9:max=' + str(max_to_test_performance) + ':rows=10000:parallel=4:loadcmd=UL:fs=\|:sq=\\" -v'
	usecase = 'Performance using 10000 as rowset'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1		


        end_time = time.time()
        logger_detail.info('======================================================================================\nTOTAL : %d\nFASSED : %d\nFAILED : %d\nDURING : %ds\n',
        total_num, total_num - failed_num, failed_num, end_time - start_time)
        logger_sumary.info('======================================================================================\nTOTAL : %d\nFASSED : %d\nFAILED : %d\nDURING : %ds\n',
        total_num, total_num - failed_num, failed_num, end_time - start_time)

create_output()

#start test
print "======================================================================================"
print "now start odb test ..."
print 
main()


