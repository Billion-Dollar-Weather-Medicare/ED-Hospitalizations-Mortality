# ED-Hospitalizations-Mortality
Repository for code to analyze Medicare utilization and outcomes following billion dollar weather disasters

Data Used:

-Medicare MBSF file - to identify sample, get death information (not publicly available)
-Medicare Inpatient & Outpatient claims - to get ED visits and urgent/emergent hospitalizations (not publicly available)
-2015 Area Health Resource File - for county characteristics (publicly available: https://data.hrsa.gov/data/download)
-Billion Dollar Weather Disasters and Counties from SHELDUS - establishes counties affected by each disaster and disaster start date to determine time periods for analysis (not publicly available) 

Brief Description of Programs:

0a_prep for matching.sas : Creates file of all counties eligible to be selected as controls
0b_county_event_matching.R : Assigns up to 5 control counties for each affected county
0c_define_time_periods.sas : Formats county-event lists with pre/post records for each time period, adds in beneficiary count to be used for bene-day offset in models  

1_ED_visits.sas : Identifies ED visits from claims and runs Negative Binomial models using proc glimmix
2_Urgent_Emergent_hosp.sas : Identifies urgent/emergent hospitalizations from claims and runs Negative Binomial models using proc glimmix
3_Mortality.sas : Identifies deaths from MBSF and runs Negative Binomial models using proc glimmix
4_outcomes_by_quartile.sas : Divides affected counties into quartiles based on $ of damage, and reruns above outcomes for each quartile

updated_map_affected_counties.R : Map for figure 1
forestplot_figure2.R : Forest plots for figure 2
