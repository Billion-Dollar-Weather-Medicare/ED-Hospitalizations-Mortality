libname d '.';
libname out '.';
libname out2 '.';

proc import datafile="E:\data\cms_aha_l3\Extreme Weather\Data\Climate Disaster Data with 2009.xlsx" out=climate_data dbms=xlsx replace; run;

data climate_data;
	set climate_data;

	where start_date ge '01JAN2011'd;

	total_damage = sum(of PropertyDmg_ADJ_2016_ CropDmg_ADJ_2016_);
run;

proc rank data=climate_data out=climate_data_damage groups=4;
	var total_damage;
	ranks q_damage;
run;

proc sort data=d.counties_with_event_dates out=event_numbers(keep=name_of_event event_number) nodupkey; by name_of_event event_number ; run;

proc sql;
	create table all_event_num as
	select a.*, b.Event_Number
	from climate_data_damage a inner join event_numbers b
	on a.Name_of_Event=b.Name_of_Event;
quit;

data all_event_num(keep=Event_Number County_FIPS damage_quart);
	set all_event_num;

	County_FIPS=compress(County_FIPS, "'");

	if q_damage=0 then damage_quart="Q1";
	if q_damage=1 then damage_quart="Q2";
	if q_damage=2 then damage_quart="Q3";
	if q_damage=3 then damage_quart="Q4";
run;


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

proc sql;
	create table all_matchgroup as
	select a.*, b.damage_quart
	from all_matched_counties a inner join all_event_num b
	on a.county_char=b.county_fips and a.event_number=b.event_number;
quit;

%macro by_time(timeframe, q);
proc sql;
	create table out.ed&q._&timeframe. as
	select *
	from out.ed_&timeframe._final
	where matchgroup in (select matchgroup from all_matchgroup where damage_quart="&q.");
quit;

proc glimmix data=out.ed&q._&timeframe. MAXOPT=200;
		class County_Type(Ref="Control") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_ed_visits = County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	lsmeans County_Type*Pre_Post / om ilink;
	ods output ParameterEstimates=out2.t&q._betas_ed_&timeframe. LSMeans=out2.t&q._lsm_ed_&timeframe.;
run;

proc glimmix data=out.ed&q._&timeframe. MAXOPT=100;
	class County_Type(Ref="Affected") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_ed_visits = County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	ods output ParameterEstimates=out2.t&q._betas2_ed_&timeframe.;
run;
%mend by_time;

%by_time(Period1, Q1);
%by_time(Period1, Q2);
%by_time(Period1, Q3);
%by_time(Period1, Q4);

%by_time(Period2, Q4);
%by_time(Period3, Q4);

%macro by_time(timeframe, q);
proc sql;
	create table out.deaths&q._&timeframe. as
	select *
	from out.deaths_&timeframe._final
	where matchgroup in (select matchgroup from all_matchgroup where damage_quart="&q.");
quit;

proc glimmix data=out.deaths&q._&timeframe. MAXOPT=200;
		class County_Type(Ref="Control") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_deaths = County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	lsmeans County_Type*Pre_Post / om ilink;
	ods output ParameterEstimates=out2.t&q._betas_mort_&timeframe. LSMeans=out2.&q._lsm_mort_&timeframe.;
run;

proc glimmix data=out.deaths&q._&timeframe. MAXOPT=200;
	class County_Type(Ref="Affected") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_deaths = County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	ods output ParameterEstimates=out2.t&q._betas2_mort_&timeframe.;
run;
%mend by_time;

%by_time(Period1, Q1);
%by_time(Period1, Q2);
%by_time(Period1, Q3);
%by_time(Period1, Q4);
%by_time(Period2, Q4);
%by_time(Period3, Q4);


%macro by_time(timeframe, q);
proc sql;
	create table out.ip&q._&timeframe. as
	select *
	from out.ip_&timeframe._final
	where matchgroup in (select matchgroup from all_matchgroup where damage_quart="&q.");
quit;

proc glimmix data=out.ip&q._&timeframe. MAXOPT=100;
		class County_Type(Ref="Control") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_ip_hosp = County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	lsmeans County_Type*Pre_Post / om ilink;
	ods output ParameterEstimates=out2.t&q._betas_ip_&timeframe. LSMeans=out2.t&q._lsm_ip_&timeframe.;
run;

proc glimmix data=out.ip&q._&timeframe. MAXOPT=100;
	class County_Type(Ref="Affected") Pre_Post(Ref="Pre") County_FIPS matchgroup;
	model num_ip_hosp = County_Type Pre_Post County_Type*Pre_Post
							 pct_male pct_white pct_dual average_age pop_2013 mhi_2013 pct_pov_2013 pct_hsdip_2013
							 matchgroup / dist=negbin link=log offset=log_bene_days cl solution;
	ods output ParameterEstimates=out2.t&q._betas2_ip_&timeframe.;
run;
%mend by_time;

%by_time(Period1, Q1);
%by_time(Period1, Q2);
%by_time(Period1, Q3);
%by_time(Period1, Q4);
%by_time(Period2, Q4);
%by_time(Period3, Q4);
