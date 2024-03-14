package gov.cdc.etldatapipeline.person.model.dto.provider;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import gov.cdc.etldatapipeline.person.utils.DataPostProcessor;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Provider {
    @Id
    @Column(name = "person_uid")
    private Long personUid;
    @Column(name = "person_parent_uid")
    private Long personParentUid;
    @Column(name = "description")
    private String description;
    @Column(name = "add_time")
    private String addTime;
    @Column(name = "first_nm")
    private String firstNm;
    @Column(name = "middle_nm")
    private String middleNm;
    @Column(name = "last_nm")
    private String lastNm;
    @Column(name = "nm_suffix")
    private String nmSuffix;
    @Column(name = "cd")
    private String cd;
    @Column(name = "electronic_ind")
    private String electronicInd;
    @Column(name = "last_chg_time")
    private String lastChgTime;
    @Column(name = "record_status_cd")
    private String recordStatusCd;
    @Column(name = "record_status_time")
    private String recordStatusTime;
    @Column(name = "status_cd")
    private String statusCd;
    @Column(name = "status_time")
    private String statusTime;
    @Column(name = "local_id")
    private String localId;
    @Column(name = "version_ctrl_nbr")
    private String versionCtrlNbr;
    @Column(name = "edx_ind")
    private String edxInd;
    @Column(name = "dedup_match_ind")
    private String dedupMatchInd;
    @Column(name = "add_user_id")
    private Long addUserId;
    @Column(name = "last_chg_user_id")
    private Long lastChgUserId;
    @Column(name = "PROVIDER_NAME_NESTED")
    private String name;
    @Column(name = "PROVIDER_ADDRESS_NESTED")
    private String address;
    @Column(name = "PROVIDER_TELEPHONE_NESTED")
    private String telephone;
    @Column(name = "PROVIDER_EMAIL_NESTED")
    private String email;
    @Column(name = "PROVIDER_ENTITY_ID_NESTED")
    private String entityData;
    @Column(name = "PROVIDER_ADD_AUTH_NESTED")
    private String addAuthNested;
    @Column(name = "PROVIDER_CHG_AUTH_NESTED")
    private String chgAuthNested;

    public ProviderFull processProvider() {
        ProviderFull pf = new ProviderFull().constructProviderFull(this);
        DataPostProcessor processor = new DataPostProcessor();
        try {
            processor.processPersonName(name, pf);
            processor.processPersonAddress(address, pf);
            processor.processPersonTelephone(telephone, pf);
            processor.processPersonAddAuth(addAuthNested, pf);
            processor.processPersonChangeAuth(chgAuthNested, pf);
            processor.processPersonEntityData(entityData, pf);
            processor.processPersonEmail(email, pf);

        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
        }
        return pf;
    }
}

