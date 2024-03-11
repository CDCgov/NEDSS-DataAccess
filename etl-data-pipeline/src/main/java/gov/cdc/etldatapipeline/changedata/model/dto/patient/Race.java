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
public class Race {
    private String raceCd;
    private String raceDescTxt;
    private String raceCategoryCd;
    @JsonProperty("person_uid")
    private Long personUid;

    public PersonOp updatePerson(PersonOp personOp) {
        personOp.setRaceCd(raceCd);
        personOp.setRaceCategory(raceCategoryCd);
        personOp.setRaceDesc(raceDescTxt);
        return personOp;
    }
}
