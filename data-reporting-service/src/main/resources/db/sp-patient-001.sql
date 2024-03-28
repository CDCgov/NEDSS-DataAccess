CREATE or ALTER PROCEDURE dbo.sp_patient_event @user_id_list varchar(max)
AS
BEGIN
    BEGIN TRY
        SELECT p.person_uid,
               p.person_parent_uid,
               p.description,
               p.add_time,
               p.age_reported,
               p.age_reported_unit_cd,
               p.first_nm,
               p.middle_nm,
               p.last_nm,
               p.nm_suffix,
               p.as_of_date_admin,
               p.as_of_date_ethnicity,
               p.as_of_date_general,
               p.as_of_date_morbidity,
               p.as_of_date_sex,
               p.birth_time,
               p.birth_time_calc,
               p.cd,
               p.curr_sex_cd,
               p.deceased_ind_cd,
               p.electronic_ind,
               p.ethnic_group_ind,
               p.birth_gender_cd,
               p.deceased_time,
               p.last_chg_time,
               p.marital_status_cd,
               p.record_status_cd,
               p.record_status_time,
               p.status_cd,
               p.status_time,
               p.local_id,
               p.version_ctrl_nbr,
               p.edx_ind,
               p.dedup_match_ind,
               p.speaks_english_cd,
               p.ethnic_unk_reason_cd,
               p.sex_unk_reason_cd,
               p.preferred_gender_cd,
               p.additional_gender_cd,
               p.occupation_cd,
               p.prim_lang_cd,
               p.last_chg_user_id,
               p.multiple_birth_ind,
               p.adults_in_house_nbr,
               p.birth_order_nbr,
               p.children_in_house_nbr,
               p.education_level_cd,
               p.add_user_id,
               case
                   when p.add_user_id > 0 then (select * from dbo.fn_get_user_name(p.add_user_id))
                   end          as add_user_name,
               case
                   when p.last_chg_user_id > 0 then (select * from dbo.fn_get_user_name(p.last_chg_user_id))
                   end          as last_chg_user_name,
               nested.name      AS 'patient_name',
               nested.address   AS 'patient_address',
               nested.phone     AS 'patient_telephone',
               nested.email     AS 'patient_email',
               nested.race      AS 'patient_race',
               nested.entity_id AS 'patient_entity'
        FROM nbs_odse.dbo.Person p WITH (NOLOCK)
                 OUTER apply (SELECT *
                              FROM
                                  -- address
                                  (SELECT (SELECT elp.cd                                                                 AS [addr_elp_cd],
                                                  elp.use_cd                                                             AS [addr_elp_use_cd],
                                                  pl.postal_locator_uid                                                  as [addr_pl_uid],
                                                  STRING_ESCAPE(pl.street_addr1, 'json')                                 AS [streetAddr1],
                                                  STRING_ESCAPE(pl.street_addr2, 'json')                                 AS [streetAddr2],
                                                  STRING_ESCAPE(pl.city_desc_txt, 'json')                                AS [city],
                                                  pl.zip_cd                                                              AS [zip],
                                                  pl.cnty_cd                                                             AS [cntyCd],
                                                  pl.state_cd                                                            AS [state],
                                                  pl.cntry_cd                                                            AS [cntryCd],
                                                  sc.code_desc_txt                                                       AS [state_desc],
                                                  scc.code_desc_txt                                                      AS [county],
                                                  pl.within_city_limits_ind,
                                                  case
                                                      when elp.use_cd = 'H'
                                                          then coalesce(cc.code_short_desc_txt, pl.cntry_cd)
                                                      end                                                      AS [home_country],
                                                  case when elp.use_cd = 'BIR' then cc.code_short_desc_txt end AS [birth_country]
                                           FROM nbs_odse.dbo.Entity_locator_participation elp WITH (NOLOCK)
                                                    LEFT OUTER JOIN nbs_odse.dbo.Postal_locator pl WITH (NOLOCK)
                                                                    ON elp.locator_uid = pl.postal_locator_uid
                                                    LEFT OUTER JOIN nbs_srte.dbo.State_code sc with (NOLOCK) ON sc.state_cd = pl.state_cd
                                                    LEFT OUTER JOIN nbs_srte.dbo.State_county_code_value scc with (NOLOCK)
                                                                    ON scc.code = pl.cnty_cd
                                                    LEFT OUTER JOIN nbs_srte.dbo.Country_code cc with (nolock) ON cc.code = pl.cntry_cd
                                           WHERE elp.entity_uid = p.person_uid
                                             AND elp.class_cd = 'PST'
                                             AND elp.status_cd = 'A'
                                           FOR json path, INCLUDE_NULL_VALUES) AS address) AS address,
                                  -- person phone
                                  (SELECT (SELECT tl.tele_locator_uid                                  AS [ph_tl_uid],
                                                  elp.cd                                               AS [ph_elp_cd],
                                                  elp.use_cd                                           AS [ph_elp_use_cd],
                                                  REPLACE(REPLACE(tl.phone_nbr_txt, '-', ''), ' ', '') AS [telephoneNbr],
                                                  tl.extension_txt                                     AS [extensionTxt]
                                           FROM nbs_odse.dbo.Entity_locator_participation elp WITH (NOLOCK)
                                                    JOIN nbs_odse.dbo.Tele_locator tl WITH (NOLOCK)
                                                         ON elp.locator_uid = tl.tele_locator_uid
                                           WHERE elp.entity_uid = p.person_uid
                                             AND elp.class_cd = 'TELE'
                                             AND elp.status_cd = 'A'
                                             AND tl.phone_nbr_txt IS NOT NULL
                                           FOR json path, INCLUDE_NULL_VALUES) AS phone) AS phone,
                                  -- person email
                                  (SELECT (SELECT tl.tele_locator_uid                     AS [email_tl_uid],
                                                  elp.cd                                  AS [email_elp_cd],
                                                  elp.use_cd                              AS [email_elp_use_cd],
                                                  STRING_ESCAPE(tl.email_address, 'json') AS [emailAddress]
                                           FROM nbs_odse.dbo.Entity_locator_participation elp WITH (NOLOCK)
                                                    JOIN nbs_odse.dbo.Tele_locator tl WITH (NOLOCK)
                                                         ON elp.locator_uid = tl.tele_locator_uid
                                           WHERE elp.entity_uid = p.person_uid
                                             AND elp.cd = 'NET'
                                             AND elp.class_cd = 'TELE'
                                             AND elp.status_cd = 'A'
                                             AND tl.email_address IS NOT NULL
                                           FOR json path, INCLUDE_NULL_VALUES) AS email) AS email,
                                  -- person name
                                  (SELECT (SELECT pn.person_uid                                        AS [pn_person_uid],
                                                  STRING_ESCAPE(REPLACE(pn.last_nm, '-', ' '), 'json') AS [lastNm],
                                                  soundex(pn.last_nm)                                  AS [lastNmSndx],
                                                  STRING_ESCAPE(pn.middle_nm, 'json')                  AS [middleNm],
                                                  STRING_ESCAPE(pn.first_nm, 'json')                   AS [firstNm],
                                                  soundex(pn.first_nm)                                 AS [firstNmSndx],
                                                  pn.nm_use_cd                                         AS [nm_use_cd],
                                                  pn.nm_suffix                                         AS [nmSuffix],
                                                  pn.nm_degree                                         AS [nmDegree],
                                                  pn.person_name_seq                                   AS [pn_person_name_seq],
                                                  pn.last_chg_time                                     AS [pn_last_chg_time]
                                           FROM nbs_odse.dbo.person_name pn WITH (NOLOCK)
                                           WHERE person_uid = p.person_uid
                                           FOR json path, INCLUDE_NULL_VALUES) AS name) AS name,
                                  -- person race
                                  (SELECT (SELECT pr.person_uid       AS [pr_person_uid],
                                                  pr.race_cd          AS [raceCd],
                                                  pr.race_desc_txt    AS [raceDescTxt],
                                                  pr.race_category_cd AS [raceCategoryCd],
                                                  src.code_desc_txt   AS [srte_code_desc_txt],
                                                  src.parent_is_cd    AS [srte_parent_is_cd]
                                           FROM nbs_odse.dbo.person_race pr WITH (NOLOCK)
                                                    LEFT OUTER JOIN nbs_srte.dbo.race_code src ON pr.race_cd = src.code
                                           WHERE person_uid = p.person_uid
                                           FOR json path, INCLUDE_NULL_VALUES) AS race) AS race,
                                  -- Entity id
                                  (SELECT (SELECT ei.entity_uid             AS [entity_uid],
                                                  ei.type_cd                AS [typeCd],
                                                  ei.record_status_cd       AS [recordStatusCd],
                                                  STRING_ESCAPE(
                                                          REPLACE(REPLACE(ei.root_extension_txt, '-', ''), ' ', ''),
                                                          'json')           AS [rootExtensionTxt],
                                                  ei.entity_id_seq          AS [entity_id_seq],
                                                  ei.assigning_authority_cd AS [assigning_authority_cd]
                                           FROM nbs_odse.dbo.entity_id ei WITH (NOLOCK)
                                           WHERE ei.entity_uid = p.person_uid
                                           FOR json path, INCLUDE_NULL_VALUES) AS entity_id) AS entity_id) AS nested
        WHERE p.person_uid in (SELECT value FROM STRING_SPLIT(@user_id_list, ','))
          AND p.cd = 'PAT'
    end try
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        return @ErrorMessage;
    END CATCH

end