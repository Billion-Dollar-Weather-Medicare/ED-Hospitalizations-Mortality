/*****************************************************************************************************/
/*** Program: 3_mortality.sas                                                                      ***/
/*** Created: Dec 19, 2018                                                                         ***/
/*** Description: Identify IP hospitalizations by county and identify as control/event/post-event/ ***/
/*** temporal control based on event dates for that county.                                        ***/
/*****************************************************************************************************/
libname d '.';
libname out '.';
libname out2 '.';
libname denom '.';

%macro mort_all_years();
%do yr=2010 %to 2016;

data death_&yr.(keep=bene_id death_dt);
	set denom.dnmntr&yr.;

	where death_dt ne .;
run;

/*** Combine with Patient Denom to get County - denom is already filtered to FFS only ***/
proc sql;
	create table deaths_&yr. as
	select a.*, b.County_FIPS, b.Bene, b.Age65_Bene  
	from death_&yr. a inner join d.denom_fips_&yr. b
	on a.bene_id=b.bene_id;
quit;
%end;
%mend mort_all_years;

%mort_all_years();

data combine_all_deaths;
	set deaths_2010-deaths_2016;
run;

%let timeframe=Period1;

%macro by_time(timeframe);
proc sql;
	create table out.deaths_in_&timeframe. as
	select a.*, b.*
	from combine_all_deaths a inner join out.&timeframe._county_dates b
	on a.county_fips=b.county_fips
	where a.death_dt ge b.Time_Start_Date and a.death_dt le b.Time_End_Date;
quit;

proc sql;
	create table deaths_&timeframe._tmp as
	select Event_Number, County_FIPS, County_Type, Pre_Post, count(*) as num_deaths
	from out.deaths_in_&timeframe.
	group by Event_Number, County_FIPS, County_Type, Pre_Post;
quit;

proc sql;
	create table out.deaths_&timeframe._county as
	select a.*, b.num_deaths
	from out.&timeframe._county_dates a left join deaths_&timeframe._tmp b
	on a.Event_Number=b.Event_Number and a.County_FIPS=b.County_FIPS and a.County_Type=b.County_Type and a.Pre_Post=b.Pre_Post;
quit;

data out.deaths_&timeframe._county;
	set out.deaths_&timeframe._county;

	if num_deaths=. then num_deaths=0;
run;
%mend by_time;

%by_time(Period1);
%by_time(Period2);
%by_time(Period3);
%by_time(Warning);

proc print data=out.deaths_&timeframe._county(obs=10);
run;

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
	create table deaths_&timeframe._tmp1 as
	select a.*, b.*
	from out.deaths_&timeframe._county a inner join out.ruca_county_10levels b
	on a.county_fips=b.FIPS_county;
quit;

data deaths_&timeframe._tmp1;
	set deaths_&timeframe._tmp1;

	year=year(Time_Start_Date);

	log_bene_days=log(bene_days_100pct);
run;

proc sql;
	create table out.deaths_&timeframe._final as
	select a.*, b.*
	from deaths_&timeframe._tmp1 a inner join county_char_combined b
	on a.county_fips=b.county_fips and a.year=b.year;
quit;
%mend by_time;

%by_time(Period1);
%by_time(Period2);
%by_time(Period3);
%by_time(Warning);

%macro by_time(timeframe);
title "Mortality &timeframe. Overall";
proc glimmix data=out.deaths_&timeframe._final MAXOPT=100;
		class County_Type(Ref="Control") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_deaths= County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	lsmeans County_Type*Pre_Post / om ilink;
	ods output ParameterEstimates=out2.betas_mort_all_&timeframe. LSMeans=out2.lsm_mort_all_&timeframe.;
run;

proc glimmix data=out.deaths_&timeframe._final MAXOPT=100;
	class County_Type(Ref="Affected") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_deaths = County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	ods output ParameterEstimates=out2.betas2_mort_all_&timeframe.;
run;
title;

 %macro by_type(type);
	title "Mortality &timeframe. &type";
	proc glimmix data=out.deaths_&timeframe._final MAXOPT=100;
		where type_of_event="&type.";
		class County_Type(Ref="Control") Pre_Post(Ref="Pre") County_FIPS matchgroup;
		model num_deaths= County_Type Pre_Post County_Type*Pre_Post
								 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
								 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
		lsmeans County_Type*Pre_Post / om ilink;
		ods output ParameterEstimates=out2.betas_mort_&type._&timeframe. LSMeans=out2.lsm_mort_&type._&timeframe.;
	run;

	proc glimmix data=out.deaths_&timeframe._final MAXOPT=100;
		where type_of_event="&type.";
		class County_Type(Ref="Affected") Pre_Post(Ref="Pre") County_FIPS matchgroup;
		model num_deaths = County_Type Pre_Post County_Type*Pre_Post
								 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
								 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
		ods output ParameterEstimates=out2.betas2_mort_&type._&timeframe.;
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
%by_time(Period2);
%by_time(Period3);
%by_time(Warning);

%macro by_time(timeframe);
 %macro by_type(type);
	 data out2.betas_mort_&type._&timeframe.;
		length metric $ 5 Type_of_Event $ 6 Version $ 2 timeframe $ 10;
	 	set out2.betas_mort_&type._&timeframe.;

		Type_of_Event="&type.";
		metric="Mort";
		Version="V1";
		timeframe="&timeframe.";
	run;

	data out2.betas2_mort_&type._&timeframe.;
		length metric $ 5 Type_of_Event $ 6 Version $ 2 timeframe $ 10;
	 	set out2.betas2_mort_&type._&timeframe.;

		Type_of_Event="&type.";
		metric="Mort";
		Version="V2";
		timeframe="&timeframe.";
	run;

	 data out2.lsm_mort_&type._&timeframe.;
		length Type_of_Event $ 5 metric $ 6 timeframe $ 10;
	 	set out2.lsm_mort_&type._&timeframe.;

		Type_of_Event="&type.";
		metric="Mort";
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
	set out2.betas_mort_:;

	rr_est=exp(Estimate)-1;
	rr_lower=exp(lower)-1;
	rr_upper=exp(upper)-1;
	p_value=probt;
run;

data all_betas2;
	set out2.betas2_mort_:;

	rr_est=exp(Estimate)-1;
	rr_lower=exp(lower)-1;
	rr_upper=exp(upper)-1;
	p_value=probt;
run;

data all_lsm;
	set out2.lsm_mort_:;
run;

proc export data=all_lsm(keep=Type_of_Event metric timeframe Effect county_Type Pre_Post rate rate_1k)
			outfile=".\output_results_10072022.xlsx"
			dbms=xlsx replace; Sheet="Mort_LSM";
run;

proc export data=all_betas(where=(Effect in ("County_Type", "Pre_Post", "County_Type*Pre_Post"))
						  keep=Type_of_Event metric timeframe Version Effect County_Type Pre_Post rr_est rr_lower rr_upper p_value)
			outfile="D.\output_results_10072022.xlsx"
			dbms=xlsx replace; Sheet="Mort_Betas";
run;

proc export data=all_betas2(where=(Effect in ("County_Type", "Pre_Post", "County_Type*Pre_Post"))
						  keep=Type_of_Event metric timeframe Version Effect County_Type Pre_Post rr_est rr_lower rr_upper p_value)
			outfile=".\output_results_10072022.xlsx"
			dbms=xlsx replace; Sheet="Mort_Betas2";
run;
