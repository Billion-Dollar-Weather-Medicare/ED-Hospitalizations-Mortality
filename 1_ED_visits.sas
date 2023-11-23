/***********************************************************************************************/
/*** Program: 1_ED_visits.sas                                                                ***/
/*** Created: Nov 8, 2018                                                                    ***/
/*** Description: Identify ED visits by county and identify as control/event/post-event/     ***/
/*** temporal control based on event dates for that county.                                  ***/
/*** note: doesnt include Obs visits bc we cant identify them in MedPAR.                     ***/
/***********************************************************************************************/
libname d '.';
libname out '.';
libname out2 '.';
libname op '.';
libname ip '.';
libname medpar '.';

/* loop through each year to identify ED visits in the */
%macro ed_visits_all_years();
%do yr=2010 %to 2016;
/*** Identify ED in Outpatient data ***/
data op_ed_&yr.;
   set op.Otptrev&yr.(keep=bene_id clm_id rev_cntr );

   where REV_CNTR in ('0450','0451','0452','0456','0459','0981');
run;

proc sort data=op_ed_&yr.(keep=bene_id clm_id) nodupkey; by bene_id clm_id; run;

/* need to get from_dt from claim level file (rev center file only has thru_dt) */
proc sql;
	create table op_ed2_&yr. as
	select a.*, 1 as ed_visit, b.FROM_DT, b.provider/*, b.PRNCPAL_DGNS_CD*/
	from op_ed_&yr. a inner join op.Otptclms&yr. b
	on a.clm_id=b.clm_id;
quit;

proc sql; drop table op_ed_&yr.; quit;

/*** Identify ED in Inpatient data ***/
%if &yr. ne 2010 %then %do;
	data ip_ed_&yr.;
	   set ip.Inptrev&yr.(keep=bene_id clm_id rev_cntr );

	   where REV_CNTR in ('0450','0451','0452','0456','0459','0981');
	run;

	proc sort data=ip_ed_&yr.(keep=bene_id clm_id) nodupkey; by bene_id clm_id; run;

	/* need to get from_dt from claim level file (rev center file only has thru_dt) */
	proc sql;
		create table ip_ed2_&yr. as
		select a.*, 1 as ed_visit, b.FROM_DT, b.provider/*, b.PRNCPAL_DGNS_CD*/
		from ip_ed_&yr. a inner join ip.Inptclms&yr. b
		on a.clm_id=b.clm_id;
	quit;

	proc sql; drop table ip_ed_&yr.; quit;
%end;

%if &yr.=2010 %then %do;
	data ip_ed2_&yr. (keep=bene_id from_dt provider);
		set medpar.medparsl&yr.;

		where GHOPDCD in ("0","") and SSLSSNF ne "N" and ER_AMT gt 0; /* remove medicare advantage visits, remove SNF, identify ER */

		rename ADMSNDT=from_dt PRVDRNUM=provider DGNSCD1=PRNCPAL_DGNS_CD; /* to match outpatient data and other year data */

		ed_visit=1;
	run;
%end;

/* Combine Inpatient and Outpatient ED visits */
data ed_&yr.;
	set op_ed2_&yr.(in=a) ip_ed2_&yr.(in=b);

	if a then op=1; else op=0;
	if b then ip=1; else ip=0;
run;

proc sql; drop table ip_ed2_&yr.; quit;
proc sql; drop table op_ed2_&yr.; quit;

data ed_&yr.;
	set ed_&yr.;

	/* keep only Acute Care and CAH - check with Renee on this */
	prov=substr(provider, 3, 2);

	Year=year(from_dt);

	if prov in ('00','01','02','03','04','05','06','07','08','09','13');
run;

/*** Combine with Patient Denom to get County (and filter to 5% or 20% based on year (5% for 2009)) ***/
proc sql;
	create table ed_denom_&yr. as
	select a.*, b.County_FIPS, b.Sample_Bene, b.Sample_Age65_Bene 
	from ed_&yr. a inner join (select * from d.denom_fips_&yr. where Sample_Bene=1) b
	on a.bene_id=b.bene_id;
quit;
%end;
%mend ed_visits_all_years;

%ed_visits_all_years();

data combine_all_ed;
	set ed_denom_2010-ed_denom_2016;
run;

/** Full Time Period would be Start of Pre-Period 3 to end of Post Period 3 (Days -49 to +41) **/


/** Aggregate at the county/event level for each time period **/
%macro by_time(timeframe);
proc sql;
	create table out.ed_visits_in_&timeframe. as
	select a.*, b.*
	from combine_all_ed a inner join out.&timeframe._county_dates b
	on a.county_fips=b.county_fips
	where a.FROM_DT ge b.Time_Start_Date and a.FROM_DT le b.Time_End_Date;
quit;

proc sql;
	create table ed_&timeframe._tmp as
	select Event_Number, County_FIPS, County_Type, Pre_Post, count(*) as num_ed_visits
	from out.ed_visits_in_&timeframe.
	group by Event_Number, County_FIPS, County_Type, Pre_Post;
quit;

proc sql;
	create table out.ed_&timeframe._county as
	select a.*, b.num_ed_visits
	from out.&timeframe._county_dates a left join ed_&timeframe._tmp b
	on a.Event_Number=b.Event_Number and a.County_FIPS=b.County_FIPS and a.County_Type=b.County_Type and a.Pre_Post=b.Pre_Post;
quit;

data out.ed_&timeframe._county;
	set out.ed_&timeframe._county;

	if num_ed_visits=. then num_ed_visits=0;
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
	create table ed_&timeframe._tmp1 as
	select a.*, b.*
	from out.ed_&timeframe._county a inner join out.ruca_county_10levels b
	on a.county_fips=b.FIPS_county;
quit;

data ed_&timeframe._tmp1;
	set ed_&timeframe._tmp1;

	year=year(Time_Start_Date);

	log_bene_days=log(bene_days_20pct);
run;

proc sql;
	create table out.ed_&timeframe._final as
	select a.*, b.*
	from ed_&timeframe._tmp1 a inner join county_char_combined b
	on a.county_fips=b.county_fips and a.year=b.year;
quit;
%mend by_time;

%by_time(Period1);
%by_time(Period2);
%by_time(Period3);
%by_time(Warning);


%macro by_time(timeframe);
proc glimmix data=out.ed_&timeframe._final MAXOPT=100;
		class County_Type(Ref="Control") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_ed_visits = County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	lsmeans County_Type*Pre_Post / om ilink;
	ods output ParameterEstimates=out2.betas_ed_&timeframe. LSMeans=out2.lsm_ed_&timeframe.;
run;

proc glimmix data=out.ed_&timeframe._final MAXOPT=100;
	class County_Type(Ref="Affected") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_ed_visits = County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	ods output ParameterEstimates=out2.betas2_ed_&timeframe.;
run;
%mend by_time;

%by_time(Period1);
%by_time(Period2);
%by_time(Period3);
%by_time(Warning);


%macro by_time(timeframe);
 %macro by_type(type);
 	proc glimmix data=out.ed_&timeframe._final MAXOPT=100;
		where type_of_event="&type.";
		class County_Type(Ref="Control") Pre_Post(Ref="Pre") County_FIPS matchgroup;
		model num_ed_visits = County_Type Pre_Post County_Type*Pre_Post
								 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
								 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
		lsmeans County_Type*Pre_Post / om ilink;
		ods output ParameterEstimates=out2.betas_ed_&type._&timeframe. LSMeans=out2.lsm_ed_&type._&timeframe.;
	run;

	proc glimmix data=out.ed_&timeframe._final MAXOPT=100;
		where type_of_event="&type.";
		class County_Type(Ref="Affected") Pre_Post(Ref="Pre") County_FIPS matchgroup;
		model num_ed_visits = County_Type Pre_Post County_Type*Pre_Post
								 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
								 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
		ods output ParameterEstimates=out2.betas2_ed_&type._&timeframe.;
	run;
 %mend by_type;

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


%macro by_time(timeframe);
 %macro by_type(type);
	 data out2.betas_ed_&type._&timeframe.;
		length metric $ 5 Type_of_Event $ 6 Version $ 2 timeframe $ 10;
	 	set out2.betas_ed_&type._&timeframe.;

		Type_of_Event="&type.";
		metric="ED";
		Version="V1";
		timeframe="&timeframe.";
	run;

	data out2.betas2_ed_&type._&timeframe.;
		length metric $ 5 Type_of_Event $ 6 Version $ 2 timeframe $ 10;
	 	set out2.betas2_ed_&type._&timeframe.;

		Type_of_Event="&type.";
		metric="ED";
		Version="V2";
		timeframe="&timeframe.";
	run;

	 data out2.lsm_ed_&type._&timeframe.;
		length Type_of_Event $ 5 metric $ 6 timeframe $ 10;
	 	set out2.lsm_ed_&type._&timeframe.;

		Type_of_Event="&type.";
		metric="ED";
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

proc print data=out.betas2_ed_&type._&timeframe.;
run;

data all_betas;
	set out2.betas_ed_:;

	rr_est=exp(Estimate)-1;
	rr_lower=exp(lower)-1;
	rr_upper=exp(upper)-1;
	p_value=probt;
run;

data all_betas2;
	set out2.betas2_ed_:;

	rr_est=exp(Estimate)-1;
	rr_lower=exp(lower)-1;
	rr_upper=exp(upper)-1;
	p_value=probt;
run;

data all_lsm;
	set out2.lsm_ed_:;
run;

proc export data=all_lsm(keep=Type_of_Event metric timeframe Effect county_Type Pre_Post rate rate_1k)
			outfile="output_results_09282022.xlsx"
			dbms=xlsx replace; Sheet="ED_LSM";
run;

proc export data=all_betas(where=(Effect in ("County_Type", "Pre_Post", "County_Type*Pre_Post"))
						  keep=Type_of_Event metric timeframe Version Effect County_Type Pre_Post rr_est rr_lower rr_upper p_value)
			outfile="output_results_09282022.xlsx"
			dbms=xlsx replace; Sheet="ED_Betas";
run;

proc export data=all_betas2(where=(Effect in ("County_Type", "Pre_Post", "County_Type*Pre_Post"))
						  keep=Type_of_Event metric timeframe Version Effect County_Type Pre_Post rr_est rr_lower rr_upper p_value)
			outfile="output_results_09282022.xlsx"
			dbms=xlsx replace; Sheet="ED_Betas2";
run;