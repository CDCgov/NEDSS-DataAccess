package gov.cdc.etldatapipeline.changedata.model.dto.patient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.changedata.model.dto.PersonOp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Name {
    private String lastNm;
    private String lastNmSndx;
    private String middleNm;
    private String firstNm;
    private String firstNmSndx;
    @JsonProperty("nm_use_cd")
    private String nmUseCd;
    private String nmSuffix;
    private String nmDegree;
    @JsonProperty("person_uid")
    private Long personUid;

    public PersonOp updatePerson(PersonOp patient){
        patient.setLastNm(this.lastNm);
        patient.setMiddleNm(this.middleNm);
        patient.setFirstNm(this.firstNm);
        patient.setNmSuffix(this.nmSuffix);
        return patient;
    }
}
