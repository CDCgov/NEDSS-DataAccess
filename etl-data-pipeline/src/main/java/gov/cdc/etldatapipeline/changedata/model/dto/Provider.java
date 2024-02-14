package gov.cdc.etldatapipeline.changedata.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import gov.cdc.etldatapipeline.changedata.model.odse.DebeziumMetadata;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.*;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class Provider extends DebeziumMetadata {
    @Id
    @Column(name = "PROVIDER_UID")
    private String providerUid;
    @Column(name = "PROVIDER_LOCAL_ID")
    private String providerLocalId;
    @Column(name = "PROVIDER_GENERAL_COMMENTS")
    private String providerGeneralComments;
    @Column(name = "PROVIDER_ENTRY_METHOD")
    private String providerEntryMethod;
    @Column(name = "PROVIDER_MPR_UID")
    private String providerMprUid;
    @Column(name = "PROVIDER_LAST_CHANGE_TIME")
    private String providerLastChangeTime;
    @Column(name = "PROVIDER_ADD_TIME")
    private String providerAddTime;
    @Column(name = "PROVIDER_RECORD_STATUS")
    private String providerRecordStatus;
    @Column(name = "ADD_USER_ID")
    private String addUserId;
    @Column(name = "LAST_CHG_USER_ID")
    private String lastChgUserId;
}

