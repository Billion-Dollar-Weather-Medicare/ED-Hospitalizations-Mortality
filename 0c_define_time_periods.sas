/***********************************************************************************************/
/*** Program: 0c_define_time_periods.sas                                                     ***/
/*** Created: Nov 1, 2018                                                                    ***/
/*** Purpose: Reads in count matches for each event, creates records for pre and post 	     ***/
/***	      for each county-event pair with relevant dates and beneficiary sample 	     ***/
/***	      of the county								     ***/
/***********************************************************************************************/

libname d '.';
libname out '.';
libname out2 '.';

/*** Identify every event that affects each county ***/
/* Read in 1) Data with County/Event Combinations and 2) Dates, Type of Event, and Event_Number for each event */
proc import datafile=".\Climate Disaster Data with 2009.xlsx" dbms=xlsx out=disaster_data replace; run;
proc import datafile=".\Extreme Weather Event Dates.xlsx" dbms=xlsx out=events_with_dates replace; run;

data disaster_data;
   set disaster_data(keep=Name_of_Event County_FIPS );

   County_FIPS=left(compress(County_FIPS,"'"));
run;

proc sort data=disaster_data nodupkey; by Name_of_Event County_FIPS; run;

proc sql;
	create table out.counties_dates as 
	select a.*, b.*
	from disaster_data a inner join events_with_dates b /* events from 2006 and 2007 will drop out - this is fine */
	on a.Name_of_Event=b.Name_of_Event;
quit;

%macro by_year(yr);
proc sql;
	create table county_sample_&yr. as
	select County_FIPS, &yr. as year, sum(Sample_Bene) as num_sample_bene, sum(Bene) as num_100pct_bene
	from d.denom_fips_&yr.
	group by county_fips;
quit;
%mend by_year;

%by_year(2010);
%by_year(2011);
%by_year(2012);
%by_year(2013);
%by_year(2014);
%by_year(2015);
%by_year(2016);

data county_sample_all;
	set county_sample_2010-county_sample_2016;
run;

/*** read in match counties by event and combine all ***/
%macro get_match(enum);
/* identify affected counties and unaffected counties without conflicting events */
proc import datafile=".\county_matched_&enum..csv" out=matched_&enum. dbms=csv replace; run;

data matched_&enum.;
	set matched_&enum.(keep=county_fips Affected subclass) ;

	Event_Number=&enum.;

	length County_Type $ 12;
	if Affected=1 then County_Type="Affected";
	if Affected=0 then County_Type="Control";

	matchgroup=catx("_",&enum.,subclass);
run;
%mend get_match;

%get_match(19);
%get_match(22);
%get_match(23);
%get_match(25);
%get_match(21);
%get_match(27);
%get_match(24);
%get_match(28);
%get_match(29);
%get_match(30);
%get_match(32);
%get_match(33);
%get_match(34);
%get_match(36);
%get_match(38);
%get_match(39);
%get_match(40);
%get_match(41);
%get_match(43);
%get_match(44);
%get_match(45);
%get_match(46);
%get_match(47);
%get_match(48);
%get_match(49);
%get_match(50);
%get_match(52);
%get_match(53);
%get_match(54);
%get_match(55);
%get_match(56);
%get_match(57);
%get_match(58);
%get_match(60);
%get_match(61);
%get_match(62);
%get_match(63);
%get_match(64);
%get_match(65);
%get_match(68);
%get_match(69);
%get_match(70);

data all_matched_counties(drop=Affected subclass );
	set matched_:;

	county_char=put(county_fips, z5.);
run;


/*** format county timeperiods/days (with logic for time periods that span two years) ***/
proc sort data=out.counties_dates
		  out=event_dates(drop=county_fips rename=(PreEmp_Pre_Start=Warning_Pre_Start PreEmp_Pre_End=Warning_Pre_End
												   PreEmp_Post_Start=Warning_Post_Start PreEmp_Post_End=Warning_Post_End)) nodupkey;
		  by event_number;
run;

%macro by_time(timeframe);
/* Pre */
data &timeframe._pre_tmp;
	set event_dates(keep=Name_of_Event Event_Number Type_of_Event &timeframe._pre_start &timeframe._pre_end);

	year_start=year(&timeframe._pre_start);
	year_end=year(&timeframe._pre_end);

	if year_start ne year_end then mult_year=1;

	length Pre_Post $ 4;
	Pre_Post="Pre";
	Timeframe="&timeframe.";

	Time_Start_Date=&timeframe._pre_start;
	Time_End_Date=&timeframe._pre_end;
	format Time_Start_Date Time_End_Date Date9.;
run;

data get_d_&timeframe._pre;
	set &timeframe._pre_tmp(in=a)
		&timeframe._pre_tmp(where=(mult_year=1) in=b);

	if a then year_count=1;
	if b then year_count=2;

	if year_count=1 then year=year_start;
	if year_count=2 then year=year_end;

	if mult_year ne 1 then days=&timeframe._pre_end-&timeframe._pre_start + 1;
	if year_count=1 and mult_year=1 then days=mdy(12,31,year_start)-&timeframe._pre_start + 1;
	if year_count=2 and mult_year=1 then days=&timeframe._pre_end-mdy(1,1,year_end) + 1;
run;

proc sql;
	create table &timeframe._pre_tmp2 as
	select a.*, b.*
	from all_matched_counties a inner join get_d_&timeframe._pre b
	on a.Event_Number=b.Event_Number;
quit;

proc sql;
	create table &timeframe._pre_tmp3 as
	select a.*, b.*
	from &timeframe._pre_tmp2(drop=county_fips) a inner join county_sample_all b
	on a.county_char=b.county_fips and a.year=b.year;
quit;

data &timeframe._pre_tmp3;
	set &timeframe._pre_tmp3 ;

	bene_days_20pct=days*num_sample_bene;
	bene_days_100pct=days*num_100pct_bene;
run;

proc sql;
	create table &timeframe._pre_tmp4 as
	select Event_Number, County_FIPS, County_Type, Pre_Post, matchgroup, Type_of_Event, Time_Start_Date, Time_End_Date, mult_year,
			sum(bene_days_20pct) as bene_days_20pct, sum(bene_days_100pct) as bene_days_100pct
	from &timeframe._pre_tmp3
	group by Event_Number, County_FIPS, County_Type, Pre_Post, matchgroup, Type_of_Event,  Time_Start_Date, Time_End_Date, mult_year 
	;
quit;

/* Post */
data &timeframe._post_tmp;
	set event_dates(keep=Name_of_Event Event_Number Type_of_Event &timeframe._post_start &timeframe._post_end);

	year_start=year(&timeframe._post_start);
	year_end=year(&timeframe._post_end);

	if year_start ne year_end then mult_year=1;

	length Pre_Post $ 4;
	Pre_Post="Post";
	Timeframe="&timeframe.";

	Time_Start_Date=&timeframe._post_start;
	Time_End_Date=&timeframe._post_end;
	format Time_Start_Date Time_End_Date Date9.;
run;

data get_d_&timeframe._post;
	set &timeframe._post_tmp(in=a)
		&timeframe._post_tmp(where=(mult_year=1) in=b);

	if a then year_count=1;
	if b then year_count=2;

	if year_count=1 then year=year_start;
	if year_count=2 then year=year_end;

	if mult_year ne 1 then days=&timeframe._post_end-&timeframe._post_start + 1;
	if year_count=1 and mult_year=1 then days=mdy(12,31,year_start)-&timeframe._post_start + 1;
	if year_count=2 and mult_year=1 then days=&timeframe._post_end-mdy(1,1,year_end) + 1;
run;

proc sql;
	create table &timeframe._post_tmp2 as
	select a.*, b.*
	from all_matched_counties a inner join get_d_&timeframe._post b
	on a.Event_Number=b.Event_Number;
quit;

proc sql;
	create table &timeframe._post_tmp3 as
	select a.*, b.*
	from &timeframe._post_tmp2(drop=county_fips) a inner join county_sample_all b
	on a.county_char=b.county_fips and a.year=b.year;
quit;

data &timeframe._post_tmp3;
	set &timeframe._post_tmp3 ;

	bene_days_20pct=days*num_sample_bene;
	bene_days_100pct=days*num_100pct_bene;
run;

proc sql;
	create table &timeframe._post_tmp4 as
	select Event_Number, County_FIPS, County_Type, Pre_Post, matchgroup, Type_of_Event, Time_Start_Date, Time_End_Date, mult_year,
			sum(bene_days_20pct) as bene_days_20pct, sum(bene_days_100pct) as bene_days_100pct
	from &timeframe._post_tmp3
	group by Event_Number, County_FIPS, County_Type, Pre_Post, matchgroup, Type_of_Event, Time_Start_Date, Time_End_Date, mult_year 
	;
quit;

data out2.&timeframe._county_dates;
	set &timeframe._pre_tmp4 &timeframe._post_tmp4;
run;
%mend by_time;

%by_time(Period1);
%by_time(Period2);
%by_time(Period3);
%by_time(Warning);
