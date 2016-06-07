CREATE OR REPLACE PROCEDURE HIXPRC.FmapPhase3Migration (  commitThreshold IN  BIGINT ,  maxRecToProcess IN BIGINT ) as

	  R_APPLN_ID VARCHAR(30);
	  R_ENRT_ID DECIMAL(12);
	  R_PRSN_MBRSH_ID DECIMAL(12);
	  R_PRSN_ENRT_ID DECIMAL(12);
	  COUNT_IND DECIMAL(12);	
	-- mapping to T_AU_FMAP table
	   R_Not_Newly_Eligible DECIMAL(12);	
	   R_Descriptive_Cohort DECIMAL(12);
	   R_EMS_Coverage_Group DECIMAL(12);
	 -- mapping to T_HIX_ASST_UNIT
	   R_INSTNLD_IND VARCHAR(1);
     -- Threshold commit count variable	
	   commit_count BIGINT DEFAULT 0;
	   cutOff_Count BIGINT DEFAULT 0;
	 -- number record count
	   record_count BIGINT DEFAULT 0;
	   --fmap_recourd count -- trigger insert or update based on the count . if count is 0 insert into fmap , else update fmap
	   fmap_record_count DECIMAL(12);
	   --overriden record count -- to find whether the app is overriden or not
	   ovrd_record_count DECIMAL(12);
	   --override_select_count -- to find whether the user has selected option during eligibility determination 
	   ovrd_select_count DECIMAL(12);
	   --prsn_enrt_count --  trigger update prsn enrt query only if there is a recored to update i.e count is greater than  0
	   prsn_enrt_count DECIMAL(12);
	   
	-- cursor for data table and process record which is new
	CURSOR fmap_data_cur (rec_to_process BIGINT ) IS  select * from HIX.T_DSS_FMAP_DATA where PRCS_STATUS_CD = 'N' and  rownum <= rec_to_process order by DSS_FMAP_DATA_ID  with ur;
	
	fmap_fetch_data fmap_data_cur%ROWTYPE;
	
	-- initialize cursor variables 
	C_PK HIX.T_DSS_FMAP_DATA.DSS_FMAP_DATA_ID%TYPE;
	C_APPLN_ID HIX.T_PRSN_MBRSH.APPLN_ID%TYPE;
	C_EMS_Coverage_Group HIX.T_REF_DATA.ref_cd%TYPE;
	C_Descriptive_Cohort HIX.T_REF_DATA.REF_CD_DESC_TX%TYPE;
	C_Not_Newly_Eligible HIX.T_REF_DATA.REF_CD_DESC_TX%TYPE;
	-- mapping to T_HIX_ASST_UNIT
	C_INSTNLD_IND HIX.T_HIX_ASST_UNIT.INSTNLD_IND_NB%TYPE;
	C_DSBLD_SIZE HIX.T_HIX_ASST_UNIT.FMAP_DSBLD_SIZE_NB%TYPE;
	C_DSBLD_INCM HIX.T_HIX_ASST_UNIT.FMAP_DSBLD_INCM_NB%TYPE;
	C_MAGI_SIZE HIX.T_HIX_ASST_UNIT.FMAP_RIBICOFF_SIZE_NB%TYPE;
	C_MAGI_INCM HIX.T_HIX_ASST_UNIT.FMAP_RIBICOFF_INCM_NB%TYPE;
	C_FIRST_NA HIX.T_DSS_FMAP_DATA.FIRST_NA%TYPE;
	C_LAST_NA HIX.T_DSS_FMAP_DATA.LAST_NA%TYPE;
	C_DOB HIX.T_DSS_FMAP_DATA.DOB_DT%TYPE;
	C_SSN_TX HIX.T_DSS_FMAP_DATA.SSN_TX%TYPE;
	C_FMAP_DETER_DATE HIX.T_AU_FMAP.FMAP_DETER_DT%TYPE;


	-- custom EXCEPTION to jump to next record when any part of code raises exception will be cached at end.
	jump_next EXCEPTION;
	value_not_found EXCEPTION;
 	-----------------------------------------------------------------------------------------
	---------------------LOGIC---------------------------------------------------------------	
	-- for each low income medicaid record in , T_DSS_FMAP_DATA
	-- if its not overriden , update  
			--T_AU_FMAP
			--T_HIX_ASST_UNIT
			--T_PRSN_ENRT
	--if its overriden update only
			--T_PRSN_ELGT
	-----------------------------------------------------------------------------------------	
BEGIN
    -- Msg buffer size
    DBMS_OUTPUT.DISABLE;
    DBMS_OUTPUT.ENABLE(1000000000);
		-- open cursor
		open fmap_data_cur(maxRecToProcess);	 
		dbms_output.put_line('---- Logging Starts with open cursor -------');
		loop
			BEGIN			
				-- We create a savepoint here. If anything goes wrong then roll-back to this checkpoint  
				SAVEPOINT savepoint_boundary ON ROLLBACK RETAIN CURSORS;
				-- fetch record from cursor
				fetch fmap_data_cur into fmap_fetch_data;
				-- exit criteria
				IF fmap_data_cur%FOUND THEN  -- fetch succeeded
						-- information retrieval from cursor
						C_PK := fmap_fetch_data.DSS_FMAP_DATA_ID; -- get PK
						DBMS_OUTPUT.PUT_LINE('Line 00: PK:' || C_PK);
				ELSE  -- fetch failed, so exit loop
						EXIT;
				END IF;
				
				BEGIN							
						C_APPLN_ID := fmap_fetch_data.APPLN_ID;
						if C_APPLN_ID is null then
							INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 02: Exception: PK:' || C_PK || ' APPLN_ID Not Found','FMAP Ph3');
							dbms_output.put_line( 'Line 02: APPLN_ID Not Found');	
							RAISE value_not_found;
						end if;
						R_APPLN_ID := C_APPLN_ID;
			
						C_FIRST_NA := fmap_fetch_data.FIRST_NA;
						if C_FIRST_NA is null then
							INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 03: Exception: PK:' || C_PK || ' FIRST_NA Not Found','FMAP Ph3');
							dbms_output.put_line( 'Line 03: FIRST_NA Not Found');	
							RAISE value_not_found;
						end if;
						C_LAST_NA := fmap_fetch_data.LAST_NA;
						if C_LAST_NA is null then 
							INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 04: Exception: PK:' || C_PK || ' LAST_NA Not Found','FMAP Ph3');
							dbms_output.put_line( 'Line 04: LAST_NA Not Found');	
							RAISE value_not_found;
						end if;	
						C_DOB := fmap_fetch_data.DOB_DT;
						if C_DOB is null then 
							INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 05: Exception: PK:' || C_PK || ' DOB Not Found','FMAP Ph3');
							dbms_output.put_line( 'Line 05: DOB Not Found');	
							RAISE value_not_found;
						end if;
						C_SSN_TX := fmap_fetch_data.SSN_TX;		
						C_EMS_Coverage_Group := fmap_fetch_data.EMS_COVERAGE_GROUP_TX;
						C_Descriptive_Cohort := fmap_fetch_data.DESCRIPTIVE_COHORT_TX;
						C_Not_Newly_Eligible := fmap_fetch_data.NOT_NEWLY_ELIG_TX;
                                    
						if C_Not_Newly_Eligible = 'TRUE' then
						  C_Not_Newly_Eligible := 'Not Newly Eligible';
						else
						  C_Not_Newly_Eligible := 'Newly Eligible';
						end if;                        
                                    
						C_INSTNLD_IND := fmap_fetch_data.IS_PRSN_INSTNLD_TX;
						C_DSBLD_SIZE := fmap_fetch_data.DSBLD_HH_SIZE_NB;					
						C_DSBLD_INCM := fmap_fetch_data.DSBLD_INCM_NB;					
						C_MAGI_SIZE := fmap_fetch_data.MAGI_HH_SIZE_NB;					
						C_MAGI_INCM := fmap_fetch_data.MAGI_INCOME_NB;					
						C_FMAP_DETER_DATE := fmap_fetch_data.FMAP_DETER_DATE;	
					EXCEPTION
						WHEN value_not_found THEN
				 		dbms_output.put_line( 'Line 12: PK:' || C_PK || ' Missing information' );
				 		RAISE jump_next;
				END;
				-- console output for data fetch
				dbms_output.put_line( 'Line 13: PK:' || C_PK || ' APPLN:' || C_APPLN_ID || ' EMS_COVG_GRP:' || C_EMS_Coverage_Group || ' DESCRIPTIVE_COHORT:' || C_Descriptive_Cohort || ' NOT_NEWLY_ELIG_COHORT:' || C_Not_Newly_Eligible );
				-------- Process record logic  -- To Get Ref Data Id-----------------------------------------------------------
				-----------------------------------------------------------------------------------------
				BEGIN
						-- get ref data id for string
						if C_EMS_Coverage_Group is not null then
							select REF_DATA_ID into R_EMS_Coverage_Group from HIX.T_REF_DATA where REF_TYPE_TX = 'EMSCoverageCodes' and REF_CD = C_EMS_Coverage_Group with ur;
						end if;	
							dbms_output.put_line( 'Line 13: EMS_Coverage_Group:' || R_EMS_Coverage_Group);        
						if C_Descriptive_Cohort is not null then
							select REF_DATA_ID into R_Descriptive_Cohort from HIX.T_REF_DATA where REF_TYPE_TX = 'Cohort' and REF_CD_DESC_TX = C_Descriptive_Cohort with ur;
						end if;
							dbms_output.put_line( 'Line 13: Descriptive_Cohort:' || R_Descriptive_Cohort);   
						if C_Not_Newly_Eligible is not null then
							select REF_DATA_ID into R_Not_Newly_Eligible from HIX.T_REF_DATA where REF_TYPE_TX = 'FMAPEligibility' and  REF_CD_DESC_TX = C_Not_Newly_Eligible with ur;
						end if;
						dbms_output.put_line( 'Line 13: Not_Newly_Eligible:' || R_Not_Newly_Eligible);   
					-- handle exception
					EXCEPTION
						WHEN Others THEN
						-- We roll back to the savepoint.
						ROLLBACK TO SAVEPOINT savepoint_boundary;
						-- insert exception to table
						INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 14: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' APPLN_ID:' || C_APPLN_ID || ' Error at getting data id from ref table.','FMAP Ph3');
						-- exception console log
						dbms_output.put_line('Line 14: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' APPLN_ID:' || C_APPLN_ID || ' Error at getting data id from ref table.');
						-- jump back to next record
						RAISE jump_next;
				END;
				-----------------------------------------------------------------------------------------
				-- console log for successful retrieval of info 
				dbms_output.put_line('Line 15: PK:' || C_PK || ' SUCCESS in Loading ref data' || ' APPLN_ID:' || C_APPLN_ID || ' EMS_COVERAGE_GROUP:' || R_EMS_Coverage_Group || ' DESCRIPTIVE_COHORT:' || R_Descriptive_Cohort || ' NOT_NEWLY_ELIG:' || R_Not_Newly_Eligible);	
				-----------------------------------------------------------------------------------------
				BEGIN					
						if C_SSN_TX is not null then 
							-- logic for application is Medicaid or not	with ssn						
							select count(distinct PRSN_MBRSH_ID) into COUNT_IND from hix.t_prsn_elgt where prsn_mbrsh_id in 	
						   		(select distinct pmbrsh.prsn_mbrsh_id from hix.t_prsn_mbrsh pmbrsh 
				                  	 join hix.t_prsn_mbrsh_prsn_name_ac pmbrname on pmbrsh.prsn_mbrsh_id = pmbrname.prsn_mbrsh_id 
				                  	 join hix.t_prsn_name pname on pname.prsn_name_id = pmbrname.prsn_name_id
				                  	 join hix.T_PRSN_ADDL_ATTR attr on pmbrsh.prsn_mbrsh_id = attr.prsn_mbrsh_id
				                  	 where lower (pname.FIRST_NA)  = lower(C_FIRST_NA) and lower(pname.LAST_NA) = lower(C_LAST_NA) and attr.DOB = C_DOB and attr.SSN_TX = C_SSN_TX
				                  	 and pmbrsh.appln_id = C_APPLN_ID)
				           		 and ((ELGT_STATUS_CD = 2100 and prog_type_cd = 440 and SUB_PROG_TYPE_CD = 2215 ) 
				            			or (OVRD_ELGT_STATUS_CD = 2100 and ovrd_prog_type_cd = 440 and ovrd_SUB_PROG_TYPE_CD = 2215 )) with ur;
							-- included distinct to avoid multiple count due to renewal and retry
							-- included condition to check ovrd medicaid and also checking eligibility status
							
							dbms_output.put_line('Line 16: PK:' || C_PK || ' Query with SSN.');
						else
							-- logic for application is Medicaid or not	without ssn
							select count(distinct PRSN_MBRSH_ID) into COUNT_IND from hix.t_prsn_elgt where prsn_mbrsh_id in 
								(select distinct pmbrsh.prsn_mbrsh_id from hix.t_prsn_mbrsh pmbrsh 
	                 					join hix.t_prsn_mbrsh_prsn_name_ac pmbrname on pmbrsh.prsn_mbrsh_id = pmbrname.prsn_mbrsh_id 
	                				  	join hix.t_prsn_name pname on pname.prsn_name_id = pmbrname.prsn_name_id
	                   					join hix.T_PRSN_ADDL_ATTR attr on pmbrsh.prsn_mbrsh_id = attr.prsn_mbrsh_id
	                   					where lower (pname.FIRST_NA)  = lower(C_FIRST_NA) and lower(pname.LAST_NA) = lower(C_LAST_NA) and attr.DOB = C_DOB
	                   					and pmbrsh.appln_id = C_APPLN_ID) 			
								and ((ELGT_STATUS_CD = 2100 and prog_type_cd = 440 and SUB_PROG_TYPE_CD = 2215) 
									or (OVRD_ELGT_STATUS_CD = 2100 and ovrd_prog_type_cd = 440 and ovrd_SUB_PROG_TYPE_CD = 2215 )) with ur;
				
							dbms_output.put_line('Line 16: PK:' || C_PK || ' Query without SSN.');
						end if;
						-----------------------------------------------------------------------------------------
						-- console log for count (0 - not medicaid / other - medicaid )
						dbms_output.put_line('Line 16: PK:' || C_PK || ' Count for 440:' || COUNT_IND);
						-----------------------------------------------------------------------------------------
						CASE COUNT_IND 
						WHEN 0 THEN
							INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 17: PK:' || C_PK || ' Application Is not Medicaid app.','FMAP Ph3');
							-----------------------------------------------------------------------------------------
							-- application is not Medicaid app do not do anything
							dbms_output.put_line('Line 17: PK:' || C_PK || ' Application Is not Medicaid app.');
							-----------------------------------------------------------------------------------------
							-- jump back to next record
							RAISE jump_next;
						ELSE
							Begin
						
							if C_SSN_TX is not null then
								 select distinct PRSN_MBRSH_ID into R_PRSN_MBRSH_ID from hix.t_prsn_elgt where prsn_mbrsh_id in 
                  					(select distinct pmbrsh.prsn_mbrsh_id from hix.t_prsn_mbrsh pmbrsh 
                 						  join hix.t_prsn_mbrsh_prsn_name_ac pmbrname on pmbrsh.prsn_mbrsh_id = pmbrname.prsn_mbrsh_id 
                 						  join hix.t_prsn_name pname on pname.prsn_name_id = pmbrname.prsn_name_id
               							  join hix.T_PRSN_ADDL_ATTR attr on pmbrsh.prsn_mbrsh_id = attr.prsn_mbrsh_id
                  							 where lower (pname.FIRST_NA)  = lower(C_FIRST_NA) and lower(pname.LAST_NA) = lower(C_LAST_NA) and attr.DOB = C_DOB and attr.SSN_TX = C_SSN_TX
                  							 and pmbrsh.appln_id = C_APPLN_ID) 
                  				and ((ELGT_STATUS_CD = 2100 and prog_type_cd = 440 and SUB_PROG_TYPE_CD = 2215 ) 
                  					or (OVRD_ELGT_STATUS_CD = 2100 and ovrd_prog_type_cd = 440 and ovrd_SUB_PROG_TYPE_CD = 2215 )) with ur;
		
							else
								select distinct PRSN_MBRSH_ID into R_PRSN_MBRSH_ID from hix.t_prsn_elgt where prsn_mbrsh_id in (select distinct pmbrsh.prsn_mbrsh_id from hix.t_prsn_mbrsh pmbrsh 
                					  	join hix.t_prsn_mbrsh_prsn_name_ac pmbrname on pmbrsh.prsn_mbrsh_id = pmbrname.prsn_mbrsh_id 
                   						join hix.t_prsn_name pname on pname.prsn_name_id = pmbrname.prsn_name_id
                   						join hix.T_PRSN_ADDL_ATTR attr on pmbrsh.prsn_mbrsh_id = attr.prsn_mbrsh_id
                  							 where lower (pname.FIRST_NA)  = lower(C_FIRST_NA) and lower(pname.LAST_NA) = lower(C_LAST_NA) and attr.DOB = C_DOB
                   							 and pmbrsh.appln_id = C_APPLN_ID) 
								and ((ELGT_STATUS_CD = 2100 and prog_type_cd = 440 and SUB_PROG_TYPE_CD = 2215 )
									or (OVRD_ELGT_STATUS_CD = 2100 and ovrd_prog_type_cd = 440 and ovrd_SUB_PROG_TYPE_CD = 2215 )) with ur;
							end if;
							
							-- TO-DO need to check possible values other than true. in db the values are F, N, Y, NULL
							
							IF C_INSTNLD_IND = 'T' THEN
								R_INSTNLD_IND := 'Y';
								dbms_output.put_line('Line 18: PK:' || C_PK || ' INSTNLD_IND:' || C_INSTNLD_IND || '-' || R_INSTNLD_IND );
							ELSIF C_INSTNLD_IND = 'F' THEN
								R_INSTNLD_IND := 'F';
								dbms_output.put_line('Line 18: PK:' || C_PK || ' INSTNLD_IND:' || C_INSTNLD_IND || '-' || R_INSTNLD_IND );
							ELSIF C_INSTNLD_IND is null THEN  
								R_INSTNLD_IND := null;
								dbms_output.put_line('Line 18: PK:' || C_PK || ' INSTNLD_IND:' || C_INSTNLD_IND || '-' || R_INSTNLD_IND);								
							ELSE
								R_INSTNLD_IND := 'N';                        
								dbms_output.put_line('Line 18: PK:' || C_PK || ' INSTNLD_IND:' || C_INSTNLD_IND || '-' || R_INSTNLD_IND );
							END IF;
							
							-- set overriden appln count -- if 0 - its not overriden app, else its overriden
							Select count(pel.prsn_elgt_id) into ovrd_record_count from 
									hix.t_prsn_elgt pel  where pel.prsn_mbrsh_id = R_PRSN_MBRSH_ID and pel.OVRD_ELGT_STATUS_CD = 2100 and pel.ovrd_prog_type_cd = 440 and pel.ovrd_SUB_PROG_TYPE_CD = 2215  and pel.ovr_in = 'Y';
							-- set fmap count-- if 0 there is no fmap record, so insert new record, else update existing record
							Select count(fmap.ASST_UNIT_ID) into fmap_record_count from  hix.t_prsn_elgt pel inner join HIX.T_AU_FMAP fmap on fmap.ASST_UNIT_ID = pel.ASST_UNIT_ID
								 where pel.prsn_mbrsh_id = R_PRSN_MBRSH_ID and pel.ELGT_STATUS_CD = 2100 and pel.PROG_TYPE_CD = 440 
									 and pel.SUB_PROG_TYPE_CD = 2215;
							-- set overriden selected count -- if 0 - override is not selected, else overriden program is selected
							Select count(pel.prsn_elgt_id) into ovrd_select_count from HIX.T_prsn_elgt pel  
									where pel.prsn_mbrsh_id = R_PRSN_MBRSH_ID and pel.OVR_IN = 'Y' and pel.OVRD_SLCTD_IN = 'Y' and pel.ovrd_ELGT_STATUS_CD = 2100 and pel.OVRD_PROG_TYPE_CD = 440 and pel.OVRD_SUB_PROG_TYPE_CD = 2215;
							-- set person enrollment count -- if 0 - no person enrt record to update, else there is record in t_prsn_enrt to update
							Select count(pe.prsn_enrt_id) into prsn_enrt_count from hix.t_prsn_elgt pel, HIX.T_prsn_enrt pe 
								where pe.PRSN_ELGT_ID = pel.PRSN_ELGT_ID and pel.prsn_mbrsh_id = R_PRSN_MBRSH_ID 
								and ((pel.ELGT_STATUS_CD = 2100 and pel.PROG_TYPE_CD = 440 and pel.SUB_PROG_TYPE_CD = 2215) 
									or (pel.ovrd_ELGT_STATUS_CD = 2100 and pel.OVRD_PROG_TYPE_CD = 440 and pel.OVRD_SUB_PROG_TYPE_CD = 2215));
							dbms_output.put_line('Line 18: PK:' || C_PK || ' fmap_record_count: ' || fmap_record_count || ' ovrd_record_count: '  || ovrd_record_count || 'ovrd_select_count' || ovrd_select_count || ' prsn_enrt_count: ' || prsn_enrt_count);
							
							--Table 1 (T_AU_FMAP) insertion
							-- Insert record only if its not overriden app (ovridden record count = 0) and no entry in au fmap table (fmap record count = 0)
							Begin
									IF (ovrd_record_count = 0 AND fmap_record_count = 0) THEN								
										Insert into HIX.T_AU_FMAP (ASST_UNIT_ID,COHORT_CD,EMS_COVG_GRP_CD,FMAP_DETER_DT,FMAP_ELGT_CD,ACTIVE_IN,CRTD_DT)
											Select pel.ASST_UNIT_ID, R_Descriptive_Cohort,R_EMS_Coverage_Group,C_FMAP_DETER_DATE,R_Not_Newly_Eligible,'Y',sysdate
 												from hix.t_prsn_elgt pel inner join HIX.T_HIX_ASST_UNIT hixau on pel.ASST_UNIT_ID = hixau.ASST_UNIT_ID
    											 left join HIX.T_AU_FMAP fmap on hixau.ASST_UNIT_ID = fmap.ASST_UNIT_ID
    											 where fmap.ASST_UNIT_ID is null and pel.prsn_mbrsh_id = R_PRSN_MBRSH_ID and pel.ELGT_STATUS_CD = 2100 and pel.PROG_TYPE_CD = 440 and pel.SUB_PROG_TYPE_CD = 2215  and (pel.ovr_in != 'Y' or pel.ovr_in is null ) with ur;
  								
										dbms_output.put_line('Line 18: PK:' || C_PK || ' fmap_record_count: ' || fmap_record_count || ' ovrd_record_count: '  || ovrd_record_count || ' T_AU_FMAP insertion done. ');
									else
										dbms_output.put_line('Line 18: PK:' || C_PK || ' T_AU_FMAP insert not done because its overrid app. ' );
									END IF;
									EXCEPTION
										WHEN Others THEN
											-- We roll back to the savepoint.
											ROLLBACK TO SAVEPOINT savepoint_boundary;										
											-- insert exception to table
											INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 18: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' APPLN_ID:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion','FMAP Ph3');							
											-- exception console log
											dbms_output.put_line('Line 18: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' appln_id:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion');											
											-- jump back to next record
											RAISE jump_next;
							End;
							
							--Table 1 (T_AU_FMAP) update	removed with ur	
							-- Execute this query to update record only if its not overriden app (ovridden record count = 0) and there is  entry in au fmap table (fmap record count != 0)
							Begin
								IF (ovrd_record_count = 0 AND fmap_record_count <> 0) THEN
									Update HIX.T_AU_FMAP set COHORT_CD = R_Descriptive_Cohort, EMS_COVG_GRP_CD = R_EMS_Coverage_Group, 
										FMAP_DETER_DT = C_FMAP_DETER_DATE, FMAP_ELGT_CD= R_Not_Newly_Eligible, ACTIVE_IN = 'Y' 
											where ACTIVE_IN = 'Y' and ASST_UNIT_ID in (  Select pel.ASST_UNIT_ID from hix.t_prsn_elgt pel  
  inner join HIX.T_HIX_ASST_UNIT hixau on pel.ASST_UNIT_ID = hixau.ASST_UNIT_ID
  inner join HIX.T_AU_FMAP fmap on fmap.ASST_UNIT_ID = hixau.ASST_UNIT_ID
  and pel.prsn_mbrsh_id =R_PRSN_MBRSH_ID 
  and ((pel.ELGT_STATUS_CD = 2100 and pel.PROG_TYPE_CD = 440 and pel.SUB_PROG_TYPE_CD = 2215) or (pel.ovrd_ELGT_STATUS_CD = 2100 and pel.OVRD_PROG_TYPE_CD = 440 and pel.OVRD_SUB_PROG_TYPE_CD = 2215)) ) ;											
									dbms_output.put_line('Line 19: PK:' || C_PK || ' fmap_record_count: ' || fmap_record_count || ' ovrd_record_count: '  || ovrd_record_count ||  ' T_AU_FMAP update done. ');									
								else
									dbms_output.put_line('Line 19: PK:' || C_PK || ' T_AU_FMAP update not done.' );
								END IF;
								EXCEPTION
									WHEN Others THEN
										-- We roll back to the savepoint.
										ROLLBACK TO SAVEPOINT savepoint_boundary;									
										-- insert exception to table
										INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 19: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' APPLN_ID:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion' ,'FMAP Ph3');									
										-- exception console log
										dbms_output.put_line('Line 19: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' appln_id:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion');										
										-- jump back to next record
										RAISE jump_next;
							End;							

							----- Table 2 (T_HIX_ASST_UNIT) updates ------------------------------
							-- update only if its not overriden app (ovridden record count = 0)
							Begin
								IF (ovrd_record_count = 0) THEN
									update hix.T_HIX_ASST_UNIT set INSTNLD_IND_NB = R_INSTNLD_IND, FMAP_DSBLD_SIZE_NB = C_DSBLD_SIZE, FMAP_DSBLD_INCM_NB = C_DSBLD_INCM, 
										FMAP_RIBICOFF_SIZE_NB = C_MAGI_SIZE, FMAP_RIBICOFF_INCM_NB = C_MAGI_INCM 
											where ASST_UNIT_ID in (Select pel.ASST_UNIT_ID from hix.t_prsn_elgt pel 
																where pel.prsn_mbrsh_id =R_PRSN_MBRSH_ID and pel.PROG_TYPE_CD = 440 and pel.SUB_PROG_TYPE_CD = 2215 
																	and pel.ELGT_STATUS_CD = 2100 and ( pel.ovr_in is null or pel.ovr_in != 'Y') with ur);									
									dbms_output.put_line('Line 20: PK:' || C_PK || ' T_HIX_ASST_UNIT update done. ' );
								else
									dbms_output.put_line('Line 20: PK:' || C_PK || ' T_HIX_ASST_UNIT update not done because its overrid app. ' );
								END IF;
								EXCEPTION
									WHEN Others THEN
										-- We roll back to the savepoint.
										ROLLBACK TO SAVEPOINT savepoint_boundary;										
										-- insert exception to table
										INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 20: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' APPLN_ID:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion','FMAP Ph3');										
										-- exception console log
										dbms_output.put_line('Line 20: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' appln_id:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion');											
										-- jump back to next record
										RAISE jump_next;
							End;
							----- Table 3 (T_PRSN_ENRT) updates ------------------------------
							-- update only if there is a record to update (prsn_enrt_count != 0)
							Begin
								IF (prsn_enrt_count <> 0) THEN
								update hix.T_PRSN_ENRT set CURR_EMS_COV_GRP_CD = R_EMS_Coverage_Group where PRSN_ELGT_ID in (
									Select pel.prsn_elgt_id from hix.t_prsn_elgt pel 
									where  pel.prsn_mbrsh_id =R_PRSN_MBRSH_ID 
										and ((pel.ELGT_STATUS_CD = 2100 and pel.PROG_TYPE_CD = 440 and pel.SUB_PROG_TYPE_CD = 2215) 
											or (pel.ovrd_ELGT_STATUS_CD = 2100 and pel.OVRD_PROG_TYPE_CD = 440 and pel.OVRD_SUB_PROG_TYPE_CD = 2215)) );
								END IF;
								
								EXCEPTION
									WHEN Others THEN
										-- We roll back to the savepoint.
										ROLLBACK TO SAVEPOINT savepoint_boundary;	
										-- insert exception to table
										INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 21: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' APPLN_ID:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion','FMAP Ph3');
										-- exception console log
										dbms_output.put_line('Line 21: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' appln_id:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion');										
										-- jump back to next record
										RAISE jump_next;
							End;						
							dbms_output.put_line('Line 21: PK:' || C_PK || ' T_PRSN_ENRT update done. ' );
							----- Table 4 (T_PRSN_ELGT) updates ------------------------------
							-- update only if its  overriden app (ovridden record count != 0)
							Begin
								IF (ovrd_record_count <> 0 AND ovrd_select_count <> 0 ) THEN
								
									update HIX.T_prsn_elgt set OVRD_FMAP_ELGT_CD = R_Not_Newly_Eligible , OVRD_COHORT_CD = R_Descriptive_Cohort, OVRD_EMS_COV_GRP_CD = R_EMS_Coverage_Group  
										where prsn_mbrsh_id = R_PRSN_MBRSH_ID and OVR_IN = 'Y' and ovrd_ELGT_STATUS_CD = 2100 and OVRD_PROG_TYPE_CD = 440 and OVRD_SUB_PROG_TYPE_CD = 2215 with ur;
									
									dbms_output.put_line('Line 22: PK:' || C_PK || ' T_PRSN_ELGT update done. Its an overrid app. ' );
								else
									dbms_output.put_line('Line 22: PK:' || C_PK || ' T_PRSN_ELGT update not done. Its not an overrid app. ' );
								END IF;
								EXCEPTION
									WHEN Others THEN
										-- We roll back to the savepoint.
										ROLLBACK TO SAVEPOINT savepoint_boundary;
										
										-- insert exception to table
										INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 22: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' APPLN_ID:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion','FMAP Ph3');
										
										-- exception console log
										dbms_output.put_line('Line 22: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' appln_id:' || C_APPLN_ID || ' Error at Table 1 (T_AU_FMAP) insertion');
											
										-- jump back to next record
										RAISE jump_next;
							End;
						
						dbms_output.put_line( '---------- App is merged  ----------');	
					End;
					END CASE;
					
					-- handle exception
						EXCEPTION
						WHEN Others THEN
							-- We roll back to the savepoint.
							--ROLLBACK TO SAVEPOINT savepoint_boundary;
						
							-- insert exception to table
							INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 24: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' Application is Medicaid or not ?' ,'FMAP Ph3');
							
							-- exception console log
							dbms_output.put_line('Line 24: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' Failed in Application is Medicaid or not ? ' );
							
							-- jump back to next record
							RAISE jump_next;
					
				END;
					
					dbms_output.put_line('Line 25: PK:' || C_PK || ' Before T_DSS_FMAP_DATA update ' );
					
					BEGIN
				
						-- update flag is processed to 'Y' in T_DSS_FMAP_DATA
						UPDATE HIX.T_DSS_FMAP_DATA SET PRCS_STATUS_CD = 'Y', updtd_dt = sysdate where DSS_FMAP_DATA_ID = C_PK;
						
						-- handle exception
						EXCEPTION
						WHEN Others THEN
							-- We roll back to the savepoint.
							ROLLBACK TO SAVEPOINT savepoint_boundary;
					
							-- insert exception to table
							INSERT INTO HIX.T_EXCEPTION_LOG (ERROR_MSG_DO,DOMAIN_OBJECT) VALUES ('Line 25: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' Failed in update flag is not processed to Y ','FMAP Ph3' );
						
							-- exception console log
							dbms_output.put_line('Line 25: PK:' || C_PK || ' SQLSTATE: ' || SQLERRM || ' update flag is not processed to Y ' );
						
							-- jump back to next record
							RAISE jump_next;
										
					END;
					
					-- console log
					dbms_output.put_line('Line 26: PK:' || C_PK || ' update flag is processed to Y ' );
					dbms_output.put_line( 'End:' || C_PK || ' ------------------------------------------------------');
					
					-----------------------------------------------
					-------- Commit threshold code ----------------
					-----------------------------------------------
					record_count := record_count + 1;
					commit_count := commit_count + 1;
					
					
					IF (commit_count = commitThreshold) THEN
						COMMIT;
						commit_count := 0;
						dbms_output.put_line( '******* Commit has executed  *******');
					END IF;	
				
					
					
					
			EXCEPTION
				WHEN jump_next THEN   
				
				-- update flag is processed to 'Y' in T_DSS_FMAP_DATA
				UPDATE HIX.T_DSS_FMAP_DATA SET PRCS_STATUS_CD = 'F', updtd_dt = sysdate where DSS_FMAP_DATA_ID = C_PK;
				
				-- SQLCODE SQLSTATE
				DBMS_OUTPUT.PUT_LINE ( 'Line 27: Jump to Next : ');
				dbms_output.put_line( 'End:' || C_PK || ' ------------------------------------------------------');					
			END;
			
			
			cutOff_Count := cutOff_Count + 1;
					
			IF (cutOff_Count = maxRecToProcess) then
				COMMIT;
				DBMS_OUTPUT.PUT_LINE ( 'Line 28: maxRecToProcess count metch. ');
				EXIT;
			END IF;	
			
		end loop;
	  
		dbms_output.put_line('---- End of Log. -------');
		dbms_output.put_line('Successfully processed:' || record_count);
	
	COMMIT;
	Close fmap_data_cur;
END; 