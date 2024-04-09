CREATE or ALTER PROCEDURE dbo.sp_Person_Event @user_id_list varchar(max)
AS

BEGIN

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
           p.add_user_id,
           p.last_chg_user_id,
           p.multiple_birth_ind,
           p.adults_in_house_nbr,
           p.birth_order_nbr,
           p.children_in_house_nbr,
           p.education_level_cd,
           nested.name      AS 'PERSON_NAME_NESTED',
           nested.address   AS 'PERSON_ADDRESS_NESTED',
           nested.phone     AS 'PERSON_TELEPHONE_NESTED',
           nested.email     AS 'PERSON_EMAIL_NESTED',
           nested.race      AS 'PERSON_RACE_NESTED',
           nested.entity_id AS 'PERSON_ENTITY_ID_NESTED',
           nested.authadd   AS 'PERSON_ADD_AUTH_NESTED',
           nested.authchg   AS 'PERSON_CHG_AUTH_NESTED'
    FROM NBS_ODSE.dbo.Person p WITH (NOLOCK)
             OUTER apply (SELECT *
                          FROM
                              -- address
                              (SELECT (SELECT pl.postal_locator_uid,
                                              elp.cd,
                                              elp.use_cd,
                                              STRING_ESCAPE(pl.street_addr1, 'json')  streetAddr1,
                                              STRING_ESCAPE(pl.street_addr2, 'json')  streetAddr2,
                                              STRING_ESCAPE(pl.city_desc_txt, 'json') city,
                                              pl.zip_cd                               zip,
                                              pl.cnty_cd                              cntyCd,
                                              pl.state_cd                             state,
                                              pl.cntry_cd                             cntryCd,
                                              sc.code_desc_txt                        state_desc,
                                              scc.code_desc_txt                       county,
                                              pl.within_city_limits_ind               within_city_limits_ind,
                                              cc.code_short_desc_txt                  country
                                       FROM Entity_locator_participation elp WITH (NOLOCK)
                                                LEFT OUTER JOIN Postal_locator pl WITH (NOLOCK)
                                                                ON elp.locator_uid = pl.postal_locator_uid
                                                LEFT OUTER JOIN NBS_SRTE.dbo.STATE_CODE sc with (NOLOCK) ON sc.STATE_CD = pl.STATE_CD
                                                LEFT OUTER JOIN NBS_SRTE.dbo.STATE_COUNTY_CODE_VALUE scc with (NOLOCK)
                                                                ON scc.CODE = pl.CNTY_CD
                                                LEFT OUTER JOIN NBS_SRTE.dbo.COUNTRY_CODE cc with (nolock) ON cc.CODE = pl.CNTRY_CD
                                       WHERE elp.entity_uid = p.person_uid
                                         AND elp.class_cd = 'PST'
                                         AND elp.status_cd = 'A'
                                       FOR json path, INCLUDE_NULL_VALUES) AS address) AS address,
                              -- person phone
                              (SELECT (SELECT tl.tele_locator_uid,
                                              elp.cd,
                                              elp.use_cd,
                                              REPLACE(REPLACE(tl.phone_nbr_txt, '-', ''), ' ', '') telephoneNbr,
                                              tl.extension_txt                                     extensionTxt
                                       FROM Entity_locator_participation elp WITH (NOLOCK)
                                                JOIN Tele_locator tl WITH (NOLOCK) ON elp.locator_uid = tl.tele_locator_uid
                                       WHERE elp.entity_uid = p.person_uid
                                         AND elp.class_cd = 'TELE'
                                         AND elp.status_cd = 'A'
                                         AND tl.phone_nbr_txt IS NOT NULL
                                       FOR json path, INCLUDE_NULL_VALUES) AS phone) AS phone,
                              -- person email
                              (SELECT (SELECT tl.tele_locator_uid,
                                              elp.cd,
                                              elp.use_cd,
                                              STRING_ESCAPE(tl.email_address, 'json') emailAddress
                                       FROM Entity_locator_participation elp WITH (NOLOCK)
                                                JOIN Tele_locator tl WITH (NOLOCK) ON elp.locator_uid = tl.tele_locator_uid
                                       WHERE elp.entity_uid = p.person_uid
                                         AND elp.class_cd = 'TELE'
                                         AND elp.status_cd = 'A'
                                         AND tl.email_address IS NOT NULL
                                       FOR json path, INCLUDE_NULL_VALUES) AS email) AS email,
                              -- person_names
                              (SELECT (SELECT pn.person_uid,
                                              STRING_ESCAPE(REPLACE(pn.last_nm, '-', ' '), 'json') lastNm,
                                              soundex(pn.last_nm)                                  lastNmSndx,
                                              STRING_ESCAPE(pn.middle_nm, 'json')                  middleNm,
                                              STRING_ESCAPE(pn.first_nm, 'json')                   firstNm,
                                              soundex(pn.first_nm)                                 firstNmSndx,
                                              pn.nm_use_cd,
                                              pn.nm_suffix                                         nmSuffix,
                                              pn.nm_degree                                         nmDegree
                                       FROM person_name pn WITH (NOLOCK)
                                       WHERE person_uid = p.person_uid
                                       FOR json path, INCLUDE_NULL_VALUES) AS name) AS name,
                              -- person race
                              (SELECT (SELECT pr.person_uid,
                                              pr.race_cd          raceCd,
                                              pr.race_desc_txt    raceDescTxt,
                                              pr.race_category_cd raceCategoryCd,
                                              src.code_desc_txt,
                                              src.parent_is_cd
                                       FROM Person_race pr WITH (NOLOCK)
                                                LEFT OUTER JOIN NBS_SRTE.dbo.RACE_CODE src ON pr.RACE_CD = src.CODE
                                       WHERE person_uid = p.person_uid
                                       FOR json path, INCLUDE_NULL_VALUES) AS race) AS race,
                              -- Entity id
                              (SELECT (SELECT ei.entity_uid,
                                              ei.type_cd            typeCd,
                                              ei.record_status_cd   recordStatusCd,
                                              STRING_ESCAPE(REPLACE(REPLACE(ei.root_extension_txt, '-', ''), ' ', ''),
                                                            'json') rootExtensionTxt,
                                              ei.entity_id_seq,
                                              ei.assigning_authority_cd
                                       FROM NBS_ODSE.dbo.entity_id ei WITH (NOLOCK)
                                       WHERE ei.entity_uid = p.person_uid
                                       FOR json path, INCLUDE_NULL_VALUES) AS entity_id) AS entity_id,
                              --auth user add
                              (SELECT (SELECT p.add_user_id,
                                              au.add_time,
                                              au.last_chg_time                                                               as ADD_USER_CHG_TIME,
                                              CAST((RTRIM(au.user_first_nm) + ', ' + RTRIM(au.user_last_nm)) as varchar(50)) AS PATIENT_ADDED_BY
                                       FROM NBS_ODSE.dbo.Auth_user au WITH (NOLOCK)
                                       WHERE p.add_user_id = au.NEDSS_ENTRY_ID
                                       FOR json path, INCLUDE_NULL_VALUES) AS authadd) AS authadd,
                              --auth user change
                              (SELECT (SELECT p.last_chg_user_id,
                                              au.add_time,
                                              au.last_chg_time                                                               AS LAST_CHG_USER_TIME,
                                              CAST((RTRIM(au.user_first_nm) + ', ' + RTRIM(au.user_last_nm)) as varchar(50)) AS PATIENT_LAST_UPDATED_BY
                                       FROM NBS_ODSE.dbo.Auth_user au WITH (NOLOCK)
                                       WHERE p.last_chg_user_id = au.NEDSS_ENTRY_ID
                                       FOR json path, INCLUDE_NULL_VALUES) AS authchg) AS authchg) AS nested
    WHERE p.person_uid in (SELECT value FROM STRING_SPLIT(@user_id_list, ','))


END