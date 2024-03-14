package gov.cdc.etldatapipeline.person.model.dto.persondetail;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
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

    public <T extends PersonExtendedProps> T updatePerson(T person){
        person.setLastNm(this.lastNm);
        person.setMiddleNm(this.middleNm);
        person.setFirstNm(this.firstNm);
        person.setNmSuffix(this.nmSuffix);
        return person;
    }
}
