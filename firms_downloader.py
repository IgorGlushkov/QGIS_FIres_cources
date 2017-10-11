 # -*- coding: UTF-8 -*-
__author__ = "IG,authentification block to Google FT by JOE STORY "
__copyright__ = ""
__license__ = ""
__modified__ = "## IG"

import sys
import requests
import httplib2
import csv
import time,os
import glob

######set by user#####
#Script for downloading, overlaying fires data (Firms) for Indonesia peatlands
#fire data interval
FIRE_LASTS ='24h'
#url to MODIS data
URL_MOD_FIRE_CSV = 'https://firms.modaps.eosdis.nasa.gov/active_fire/c6/text/MODIS_C6_SouthEast_Asia_%s.csv' % FIRE_LASTS
#url to VIIRS data
URL_VII_FIRE_CSV = 'https://firms.modaps.eosdis.nasa.gov/active_fire/viirs/text/VNP14IMGTDL_NRT_SouthEast_Asia_%s.csv' % FIRE_LASTS
#dirs for temporal and result files
source_dir='d:/Thematic/QGIS_cource/IND_Day1/sess8/python_auto_download/Firms_source'
source_sel='d:/Thematic/QGIS_cource/IND_Day1/sess8/python_auto_download/Firms_source/Temp'
result_dir='d:/Thematic/QGIS_cource/IND_Day1/sess8/python_auto_download/Firms_source/Result'
#filenames for polygons (peatlands from GFW Indonesia_Peat_Lands.shp)
mask = 'peatlands'
######set by user######

#def get session
def get_session(url):
	url = url
	s = requests.session()
	headers = {
		'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
		'X-Requested-With': 'XMLHttpRequest',
		'Referer': url,
		'Pragma': 'no-cache',
		'Cache-Control': 'no-cache'}
	r = s.get(url, headers=headers)
	return(r)

#read file from site and save to csv
def read_csv_from_site(url):
	r = get_session(url)
	reader = csv.reader(r.iter_lines(), delimiter=',', quotechar='"')
	outfile=open(os.path.join(source_dir, '%s.csv'%(filename)), 'wb')
	tmp = csv.writer(outfile)
	tmp.writerows(reader)

#intersect with polygons layer using ogr2ogr
def sp_join(filename):
	try:
		#convert to shp
		#create vrt and convert to shp
		f = open(os.path.join(source_dir, '%s.vrt'%(filename)), 'w')
		f.write("<OGRVRTDataSource>\n")
		f.write("  <OGRVRTLayer name=\"%s_tmp\">\n" % (filename))
		f.write("    <SrcDataSource relativeToVRT=\"1\">%s</SrcDataSource>\n" % (source_dir))
		f.write("    <SrcLayer>%s</SrcLayer>\n" % (filename))
		f.write("    <GeometryType>wkbPoint</GeometryType>\n")
		f.write("    <LayerSRS>WGS84</LayerSRS>\n")
		f.write("    <GeometryField encoding=\"PointFromColumns\" x=\"longitude\" y=\"latitude\"/>\n")
		f.write("  </OGRVRTLayer>\n")
		f.write("</OGRVRTDataSource>\n")
		f.close()
		#convert
		command="ogr2ogr -overwrite -skipfailures -f \"ESRI Shapefile\" %s %s && ogr2ogr -overwrite -f \"ESRI Shapefile\" %s %s"  % (source_dir,os.path.join(source_dir, '%s.csv'%(filename)),source_dir,os.path.join(source_dir, '%s.vrt'%(filename)))
		print(command)
		os.system(command)   
		#intersect   
		command = "ogr2ogr -overwrite -lco encoding=UTF-8 -sql \"SELECT ST_Intersection(A.geometry, B.geometry) AS geometry, A.*, B.* FROM %s_tmp A, %s B WHERE ST_Intersects(A.geometry, B.geometry)\" -dialect SQLITE %s %s -nln %s_tmp1" % (filename,mask,source_dir,source_dir,filename)
		print(command)
		os.system(command)
		#conver back to csv
		command = "ogr2ogr -overwrite -skipfailures -f  \"ESRI Shapefile\" %s %s" % (os.path.join(result_dir,'%s_peatlands.shp'%(filename)),os.path.join(source_dir, '%s_tmp1.shp'%(filename))) 
		print(command)
		os.system(command)
	except:
		print('An error occured..')

#remove files
def silent_remove(filename):
	try:
		my_files=glob.glob(os.path.join(source_dir, '%s.*'%filename))
		for f in my_files:
			if os.path.exists(filename):
				os.remove(filename)
	except:
		print ('Error removing files')
		pass
	
if __name__ == "__main__":
	while True:
		log=os.path.join(source_dir, 'log.txt')
		logf = open(log, 'a')
		#current date
		start=time.time()
		currtime = time.localtime()
		date=time.strftime('%d%m%Y',currtime)
		cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
		#set modis filename
		filename_modis = 'modis_%s' % (date)
		#set viirs filename
		filename_viirs = 'viirs_%s' % (date)
		print 'Process started at %s'%(cdate)
		logf.write('Process started at %s\n'%(cdate))
		#create dictionary 
		sat_url={filename_modis:URL_MOD_FIRE_CSV,filename_viirs:URL_VII_FIRE_CSV}
		#start workflow for modis and viirs 
		for filename,url in sat_url.iteritems():
			read_csv_from_site(url)
			sp_join(filename)
			silent_remove(filename)
		end=time.time()
		currtime = time.localtime()
		cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
		sleep=86401-(end-start)
		logf.write('Process ended successfully at %s\n'%(cdate))
		print('Process ended successfully at %s\n'%(cdate))
		logf.close()
		time.sleep(sleep)

