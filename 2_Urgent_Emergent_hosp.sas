/*****************************************************************************************************/
/*** Program: 2_IP_hosp.sas                                                                        ***/
/*** Created: Dec 19, 2018                                                                          ***/
/*** Description: Identify IP hospitalizations by county and identify as control/event/post-event/ ***/
/*** temporal control based on event dates for that county.                                        ***/
/*****************************************************************************************************/
libname d '.';
libname out '.';
libname out2 '.';
libname op '.';
libname ip '.';
libname medpar '.';

%macro ip_all_years();
%do yr=2009 %to 2016;

/*** Identify ED/Obs in Inpatient data ***/
%if &yr. ne 2010 %then %do;
	data IP_hosp_&yr.(keep=bene_id provider ADMSN_DT Year);
		set ip.Inptclms&yr.;

		where type_adm in ('1','2'); /* only keep hospitalizations classified as urgent or emergency*/

		prov=substr(provider, 3, 2);
		Year=year(ADMSN_DT);

		if prov in ('00','01','02','03','04','05','06','07','08','09','13');
	run;
%end;

%if &yr.=2010 %then %do;
	data IP_hosp_&yr.(keep=bene_id provider ADMSN_DT Year);
		set medpar.medparsl&yr.;

		where GHOPDCD in ("0","") and SSLSSNF ne "N" and type_adm in ('1','2'); /* remove medicare advantage visits, remove SNF */

		provider=PRVDRNUM;
		ADMSN_DT=ADMSNDT;

		prov=substr(provider, 3, 2);
		Year=year(ADMSN_DT);

		if prov in ('00','01','02','03','04','05','06','07','08','09','13');
	run;
%end;

/*** Combine with Patient Denom to get County - denom is already filtered to FFS only ***/
proc sql;
	create table IP_&yr. as
	select a.*, b.County_FIPS, b.Bene, b.Age65_Bene  
	from IP_hosp_&yr. a inner join d.denom_fips_&yr. b
	on a.bene_id=b.bene_id;
quit;
%end;
%mend ip_all_years;

%ip_all_years();

data combine_all_ip;
	set IP_2010-IP_2016;
run;

%macro by_time(timeframe);
proc sql;
	create table out.ip_visits_in_&timeframe. as
	select a.*, b.*
	from combine_all_ip a inner join out.&timeframe._county_dates b
	on a.county_fips=b.county_fips
	where a.ADMSN_DT ge b.Time_Start_Date and a.ADMSN_DT le b.Time_End_Date;
quit;

proc sql;
	create table ip_&timeframe._tmp as
	select Event_Number, County_FIPS, County_Type, Pre_Post, count(*) as num_ip_hosp
	from out.ip_visits_in_&timeframe.
	group by Event_Number, County_FIPS, County_Type, Pre_Post;
quit;

proc sql;
	create table out.ip_&timeframe._county as
	select a.*, b.num_ip_hosp
	from out.&timeframe._county_dates a left join ip_&timeframe._tmp b
	on a.Event_Number=b.Event_Number and a.County_FIPS=b.County_FIPS and a.County_Type=b.County_Type and a.Pre_Post=b.Pre_Post;
quit;

data out.ip_&timeframe._county;
	set out.ip_&timeframe._county;

	if num_ip_hosp=. then num_ip_hosp=0;
run;
%mend by_time;

%by_time(Period1);
%by_time(Period2);
%by_time(Period3);
%by_time(Warning);

data county_char_combined;
	set d.county_char_new_2010(in=a)
		d.county_char_new_2011(in=b)
		d.county_char_new_2012(in=c)
		d.county_char_new_2013(in=d)
		d.county_char_new_2014(in=e)
		d.county_char_new_2015(in=f)
		d.county_char_new_2016(in=g);

	if a then year=2010;
	if b then year=2011;
	if c then year=2012;
	if d then year=2013;
	if e then year=2014;
	if f then year=2015;
	if g then year=2016;
run;

%macro by_time(timeframe);
proc sql;
	create table ip_&timeframe._tmp1 as
	select a.*, b.*
	from out.ip_&timeframe._county a inner join out.ruca_county_10levels b
	on a.county_fips=b.FIPS_county;
quit;

data ip_&timeframe._tmp1;
	set ip_&timeframe._tmp1;

	year=year(Time_Start_Date);

	log_bene_days=log(bene_days_100pct);
run;

proc sql;
	create table out.ip_&timeframe._final as
	select a.*, b.*
	from ip_&timeframe._tmp1 a inner join county_char_combined b
	on a.county_fips=b.county_fips and a.year=b.year;
quit;
%mend by_time;

%by_time(Period1);
%by_time(Period2);
%by_time(Period3);
%by_time(Warning);


%macro by_time(timeframe);
title "IP &timeframe. Overall";
proc glimmix data=out.ip_&timeframe._final MAXOPT=100;
	class County_Type(Ref="Control") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_ip_hosp = County_Type Pre_Post County_Type*Pre_Post
						pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
						matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	lsmeans County_Type*Pre_Post / om ilink;
	ods output ParameterEstimates=out2.betas_ip_all_&timeframe. LSMeans=out2.lsm_ip_all_&timeframe.;
run;

proc glimmix data=out.ip_&timeframe._final MAXOPT=100;
	class County_Type(Ref="Affected") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_ip_hosp = County_Type Pre_Post County_Type*Pre_Post
						pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
						matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	ods output ParameterEstimates=out2.betas2_ip_all_&timeframe.;
run;
title;

 %macro by_type(type);
	title "IP &timeframe. &type";
 	proc glimmix data=out.ip_&timeframe._final MAXOPT=100;
		where type_of_event="&type.";
		class County_Type(Ref="Control") Pre_Post(Ref="Pre") County_FIPS matchgroup;
		model num_ip_hosp = County_Type Pre_Post County_Type*Pre_Post
								 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
								 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
		lsmeans County_Type*Pre_Post / om ilink;
		ods output ParameterEstimates=out2.betas_ip_&type._&timeframe. LSMeans=out2.lsm_ip_&type._&timeframe.;
	run;

	proc glimmix data=out.ip_&timeframe._final MAXOPT=100;
		where type_of_event="&type.";
		class County_Type(Ref="Affected") Pre_Post(Ref="Pre") County_FIPS matchgroup;
		model num_ip_hosp = County_Type Pre_Post County_Type*Pre_Post
								 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
								 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
		ods output ParameterEstimates=out2.betas2_ip_&type._&timeframe.;
	run;
	title;
 %mend by_type;

 %by_type(FLD);
 %by_type(FLD_SS);
 %by_type(SS);
 %by_type(TC);
 %by_type(WS);
%mend by_time;

%by_time(Period1);
/*%by_time(Period2);*/
/*%by_time(Period3);*/
/*%by_time(Warning);*/

ods html close;
ods html;

%macro by_time(timeframe);
 %macro by_type(type);
	 data out2.betas_IP_&type._&timeframe.;
		length metric $ 5 Type_of_Event $ 6 Version $ 2 timeframe $ 10;
	 	set out2.betas_IP_&type._&timeframe.;

		Type_of_Event="&type.";
		metric="IP";
		Version="V1";
		timeframe="&timeframe.";
	run;

	data out2.betas2_IP_&type._&timeframe.;
		length metric $ 5 Type_of_Event $ 6 Version $ 2 timeframe $ 10;
	 	set out2.betas2_IP_&type._&timeframe.;

		Type_of_Event="&type.";
		metric="IP";
		Version="V2";
		timeframe="&timeframe.";
	run;

	 data out2.lsm_IP_&type._&timeframe.;
		length Type_of_Event $ 5 metric $ 6 timeframe $ 10;
	 	set out2.lsm_IP_&type._&timeframe.;

		Type_of_Event="&type.";
		metric="IP";
		timeframe="&timeframe.";
		rate=Mu;
		rate_1k=rate*1000;
	run;
 %mend by_type;

 %by_type(All);
 %by_type(FLD);
 %by_type(FLD_SS);
 %by_type(SS);
 %by_type(TC);
 %by_type(WS);
%mend by_time;

%by_time(Period1);
%by_time(Period2);
%by_time(Period3);
%by_time(Warning);

data all_betas;
	set out2.betas_ip_:;

	rr_est=exp(Estimate)-1;
	rr_lower=exp(lower)-1;
	rr_upper=exp(upper)-1;
	p_value=probt;
run;

data all_betas2;
	set out2.betas2_ip_:;

	rr_est=exp(Estimate)-1;
	rr_lower=exp(lower)-1;
	rr_upper=exp(upper)-1;
	p_value=probt;
run;

data all_lsm;
	set out2.lsm_ip_:;
run;

proc export data=all_lsm(keep=Type_of_Event metric timeframe Effect county_Type Pre_Post rate rate_1k)
			outfile=".\output_results_10072022.xlsx"
			dbms=xlsx replace; Sheet="IP_LSM";
run;

proc export data=all_betas(where=(Effect in ("County_Type", "Pre_Post", "County_Type*Pre_Post"))
						  keep=Type_of_Event metric timeframe Version Effect County_Type Pre_Post rr_est rr_lower rr_upper p_value)
			outfile=".\output_results_10072022.xlsx"
			dbms=xlsx replace; Sheet="IP_Betas";
run;

proc export data=all_betas2(where=(Effect in ("County_Type", "Pre_Post", "County_Type*Pre_Post"))
						  keep=Type_of_Event metric timeframe Version Effect County_Type Pre_Post rr_est rr_lower rr_upper p_value)
			outfile=".\output_results_10072022.xlsx"
			dbms=xlsx replace; Sheet="IP_Betas2";
run;
