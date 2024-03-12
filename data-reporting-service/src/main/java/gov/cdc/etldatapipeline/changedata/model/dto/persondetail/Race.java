package gov.cdc.etldatapipeline.changedata.model.dto.persondetail;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.changedata.model.dto.PersonFull;
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

    public PersonFull updatePerson(PersonFull personFull) {
        personFull.setRaceCd(raceCd);
        personFull.setRaceCategory(raceCategoryCd);
        personFull.setRaceDesc(raceDescTxt);
        return personFull;
    }
}