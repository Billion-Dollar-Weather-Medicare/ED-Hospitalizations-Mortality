/***********************************************************************************************/
/*** Program: 0a_prep_for_matching.sas                                                       ***/
/*** Purpose: For each event, identify affected counties and counties that are eligible to   ***/
/***	      be used as controls.                                                           ***/
/***	      	 -Not affected by another disasters in the 6 months leading up               ***/
/***		  to the disaster or 6 weeks after					     ***/
/***	     	 -Not bordering an affected county					     ***/
/*** 	      Output file will be used as input to matching program			     ***/
/***********************************************************************************************/
libname arf '.';
libname d '.';
libname out '.';

/*** Identify every event that affects each county ***/
/* Read in 1) Data with County/Event Combinations and 2) Dates, Type of Event, and Event_Number for each event */
proc import datafile=".\Climate Disaster Data with 2009.xlsx" dbms=xlsx out=disaster_data replace; run;
proc import datafile=".\Extreme Weather Event Dates - Final.xlsx" dbms=xlsx out=events_with_dates replace; run;

data disaster_data;
   set disaster_data(keep=Name_of_Event County_FIPS );

   County_FIPS=left(compress(County_FIPS,"'"));
run;

proc sort data=disaster_data nodupkey; by Name_of_Event County_FIPS; run;

proc sql;
	create table affected_counties as 
	select a.Name_of_Event, a.County_FIPS, b.Event_Number
	from disaster_data a inner join events_with_dates b
	on a.Name_of_Event=b.Name_of_Event;
quit;

data events_with_dates;
	set events_with_dates;

	sixmos_pre = Start_Date - 180; format sixmos_pre date9.;
	sixwk_post= Start_Date + 42; format sixwk_post date9.;
run;

/** get denominator - all counties in US **/
data all_counties(drop=f:);
	set arf.ahrf2015(keep=f00002 f04448 f0002013 f12424 f0410180--f0411480);

	where f00002 ne '' and f12424 ne 'PR';

	county_fips=f00002;
	census_region=f04448;
	rural_urban_cont=f0002013;
	state=f12424;

	cont_county1=f0410180;
	cont_county2=f0410280;
	cont_county3=f0410380;
	cont_county4=f0410480;
	cont_county5=f0410580;
	cont_county6=f0410680;
	cont_county7=f0410780;
	cont_county8=f0410880;
	cont_county9=f0410980;
	cont_county10=f0411080;
	cont_county11=f0411180;
	cont_county12=f0411280;
	cont_county13=f0411380;
	cont_county14=f0411480;
run;

proc sql;
	create table all_counties_ruca as
	select a.*, b.*
	from all_counties a inner join out.ruca_county_10levels b
	on a.county_fips=b.FIPS_county;
quit;

proc sort data=all_counties_ruca; by county_fips; run;

/*** Loop through each event and create indicator variable for if a county is affected by that event ***/
%macro get_counties();
%do i=1 %to 70;
	data tmp1(keep=County_FIPS Event&i);
		set affected_counties;

		where Event_Number=&i;
		Event&i=1;
	run;

	proc sort data=tmp1 nodupkey; by County_FIPS; run;

	data all_counties_ruca;
		merge all_counties_ruca(in=a) tmp1(in=b);
		  by County_FIPS;

		if a;

		if a and not b then Event&i=0;
	run;

	data events_with_dates&i;
		set events_with_dates;

		where Event_Number=&i;
	run;

	data events_avoid_tmp_&i.(keep=event_number);
		if _N_=1 then set events_with_dates&i(keep=sixmos_pre sixwk_post);
		set events_with_dates(keep=Event_Number Start_Date);

		if Start_Date gt sixmos_pre and Start_Date lt sixwk_post and event_number ne &i.;
	run;

	proc transpose data=events_avoid_tmp_&i. out=events_avoid_tmp2_&i.; run;

	data events_avoid_&i.(keep=event_number COL:);
		retain event_number;
		set events_avoid_tmp2_&i.;

		event_number=&i.;
	run;
%end;
%mend get_counties;

%get_counties();

data events_avoid_56;
	set events_avoid_56;

	col1=0;
run;

%macro get_prematch(enum);
/* identify affected counties and unaffected counties without conflicting events */
data affected_&enum. controltmp_&enum.;
	set all_counties_ruca;

	if Event&enum.=1 then output affected_&enum.;
	if Event&enum.=0 then output controltmp_&enum.;
run;

data controltmp2_&enum.;
	if _N_=1 then set events_avoid_&enum.(keep=col:);
	set controltmp_&enum.;

	exclude_county=0;

	array excl (*) col:;

	do i=1 to dim(excl);
		if excl(i)=18 and Event18=1 then exclude_county=1;
		 else if excl(i)=19 and Event19=1 then exclude_county=1;
		 else if excl(i)=20 and Event20=1 then exclude_county=1;
		 else if excl(i)=21 and Event21=1 then exclude_county=1;
		 else if excl(i)=22 and Event22=1 then exclude_county=1;
		 else if excl(i)=23 and Event23=1 then exclude_county=1;
		 else if excl(i)=24 and Event24=1 then exclude_county=1;
		 else if excl(i)=25 and Event25=1 then exclude_county=1;
		 else if excl(i)=26 and Event26=1 then exclude_county=1;
		 else if excl(i)=27 and Event27=1 then exclude_county=1;
		 else if excl(i)=28 and Event28=1 then exclude_county=1;
		 else if excl(i)=29 and Event29=1 then exclude_county=1;
		 else if excl(i)=30 and Event30=1 then exclude_county=1;
		 else if excl(i)=31 and Event31=1 then exclude_county=1;
		 else if excl(i)=32 and Event32=1 then exclude_county=1;
		 else if excl(i)=33 and Event33=1 then exclude_county=1;
		 else if excl(i)=34 and Event34=1 then exclude_county=1;
		 else if excl(i)=35 and Event35=1 then exclude_county=1;
		 else if excl(i)=36 and Event36=1 then exclude_county=1;
		 else if excl(i)=37 and Event37=1 then exclude_county=1;
		 else if excl(i)=38 and Event38=1 then exclude_county=1;
		 else if excl(i)=39 and Event39=1 then exclude_county=1;
		 else if excl(i)=40 and Event40=1 then exclude_county=1;
		 else if excl(i)=41 and Event41=1 then exclude_county=1;
		 else if excl(i)=42 and Event42=1 then exclude_county=1;
		 else if excl(i)=43 and Event43=1 then exclude_county=1;
		 else if excl(i)=44 and Event44=1 then exclude_county=1;
		 else if excl(i)=45 and Event45=1 then exclude_county=1;
		 else if excl(i)=46 and Event46=1 then exclude_county=1;
		 else if excl(i)=47 and Event47=1 then exclude_county=1;
		 else if excl(i)=48 and Event48=1 then exclude_county=1;
		 else if excl(i)=49 and Event49=1 then exclude_county=1;
		 else if excl(i)=50 and Event50=1 then exclude_county=1;
		 else if excl(i)=51 and Event51=1 then exclude_county=1;
		 else if excl(i)=52 and Event52=1 then exclude_county=1;
		 else if excl(i)=53 and Event53=1 then exclude_county=1;
		 else if excl(i)=54 and Event54=1 then exclude_county=1;
		 else if excl(i)=55 and Event55=1 then exclude_county=1;
		 else if excl(i)=56 and Event56=1 then exclude_county=1;
		 else if excl(i)=57 and Event57=1 then exclude_county=1;
		 else if excl(i)=58 and Event58=1 then exclude_county=1;
		 else if excl(i)=59 and Event59=1 then exclude_county=1;
		 else if excl(i)=60 and Event60=1 then exclude_county=1;
		 else if excl(i)=61 and Event61=1 then exclude_county=1;
		 else if excl(i)=62 and Event62=1 then exclude_county=1;
		 else if excl(i)=63 and Event63=1 then exclude_county=1;
		 else if excl(i)=64 and Event64=1 then exclude_county=1;
		 else if excl(i)=65 and Event65=1 then exclude_county=1;
		 else if excl(i)=66 and Event66=1 then exclude_county=1;
		 else if excl(i)=67 and Event67=1 then exclude_county=1;
		 else if excl(i)=68 and Event68=1 then exclude_county=1;
		 else if excl(i)=69 and Event69=1 then exclude_county=1;
		 else if excl(i)=70 and Event70=1 then exclude_county=1;
	end;

	if exclude_county=1 then delete;
run;

/* exclude counties that border an unaffected county */
data border_counties_&enum.;
	set affected_&enum.;

	array cnty{14} $ cont_county1-cont_county14;

	do i=1 to 14;
		if cnty{i} ne '' then do;
			border_county=cnty{i}; output;
		end;
	end;

	keep Event&enum. border_county;
run;

proc sql;
	create table control_&enum. as
	select *
	from controltmp2_&enum.
	where County_FIPs not in (select border_county from border_counties_&enum.);
quit;

data prematch_&enum.;
	set affected_&enum.(keep=Event&enum. county_fips census_region rural_urban_cont state Primary_RUCA_Code_2010 County_pop_ruca RUCA)
		control_&enum.(keep=Event&enum. county_fips census_region rural_urban_cont state Primary_RUCA_Code_2010 County_pop_ruca RUCA);

	Affected=Event&enum.;

	rand_num=ranuni(10); /* random sort so that order of counties doesn't dictate matches */
run;

proc sort data=prematch_&enum.; by rand_num; run;

proc export data=prematch_&enum. outfile=".\county_prematch_new_&enum..csv" dbms=csv replace; run;
%mend get_prematch;

%get_prematch(19);
%get_prematch(22);
%get_prematch(23);
%get_prematch(25);
%get_prematch(21);
%get_prematch(27);
%get_prematch(24);
%get_prematch(28);
%get_prematch(29);
%get_prematch(30);
%get_prematch(32);
%get_prematch(33);
%get_prematch(34);
%get_prematch(36);
%get_prematch(38);
%get_prematch(39);
%get_prematch(40);
%get_prematch(41);
%get_prematch(43);
%get_prematch(44);
%get_prematch(45);
%get_prematch(46);
%get_prematch(47);
%get_prematch(48);
%get_prematch(49);
%get_prematch(50);
%get_prematch(52);
%get_prematch(53);
%get_prematch(54);
%get_prematch(55);
%get_prematch(56);
%get_prematch(57);
%get_prematch(58);
%get_prematch(60);
%get_prematch(61);
%get_prematch(62);
%get_prematch(63);
%get_prematch(64);
%get_prematch(65);
%get_prematch(68);
%get_prematch(69);
%get_prematch(70);