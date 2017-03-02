all_data = LOAD '/cloudcomp/aws_gsod_data' USING org.apache.pig.piggybank.storage.FixedWidthLoader('
	1-6,
	8-12,
	15-18, 
	19-20,
	21-22,
	15-18, 
	19-20,
	21-22,
	25-30,
	36-41,
	47-52,
	69-73,
	79-83,
	103-108,
	111-116,
	126-130',
	'SKIP_HEADER')
	AS (stn:chararray,wban:chararray,year:chararray,month:chararray,day:chararray,yearraw:chararray,monthraw:chararray,dayraw:chararray,temp:chararray,dewp:chararray,slp:chararray,visib:chararray,wdsp:chararray,max:chararray,min:chararray,sndp:chararray);

all_data_modified = FOREACH all_data GENERATE CONCAT(stn,'',wban) AS station,
						year AS year,
						month AS month,
						day AS day,
					      (int)CONCAT(yearraw,'-',monthraw,'-',dayraw) AS date,
					     (float)(temp == '9999.9' ? '0' : temp)  AS temp,
					     (float)(dewp == '9999.9' ? '0' : dewp)  AS dewp,
					     (float)(slp == '9999.9' ? '0' : slp)  AS slp,
					     (float) (visib == '999.9' ? '0' : visib) AS visib,
					     (float) (wdsp == '999.9' ? '0' : wdsp) AS wdsp,
					    (float) (max == '9999.9' ? '0' : max)  AS max,
					    (float) (min == '9999.9' ? '0' : min)  AS min,
					    (float)  (sndp == '999.9' ? '0' : sndp) AS sndp;

grpd = GROUP all_data_modified BY (year,station);

compressed = FOREACH grpd GENERATE group.year,
					group.station,
					(float) AVG(all_data_modified.temp) AS avg_temp,
					(float)  AVG(all_data_modified.dewp) AS avg_dewp,
					(float) AVG(all_data_modified.slp) AS avg_slp,
					(float) AVG(all_data_modified.visib) AS avg_visib ,
					(float) AVG(all_data_modified.wdsp) AS avg_wdsp,
					(float) MAX(all_data_modified.max) AS max_temp,
					(float) MIN(all_data_modified.min) AS min_temp,
					(float) AVG(all_data_modified.sndp)  AS avg_sndp;



county_code= LOAD '/cloudcomp/ish-history.csv'   USING PigStorage(',') as (stn:chararray,wban:chararray,station:chararray,county:chararray);	

counry_code_modified= FOREACH county_code GENERATE CONCAT(REPLACE(stn, '\\"', ''),'',REPLACE(wban, '\\"', '')) as stationval,
							REPLACE(county, '\\"', '')	as country;						
result_join = JOIN compressed BY station, counry_code_modified BY stationval;








finalgrpd= GROUP result_join BY (year , country );



finalgrpd_changed = FOREACH finalgrpd GENERATE group.year,
					group.country,
					AVG(result_join.avg_temp),
					 AVG(result_join.avg_dewp),
					AVG(result_join.avg_slp),
					AVG(result_join.avg_visib),
					AVG(result_join.avg_wdsp),
					MAX(result_join.max_temp),
					MIN(result_join.min_temp),
					AVG(result_join.avg_sndp)  ;


STORE finalgrpd_changed INTO '/cloudcomp/gsod_data_output_pig' ;

