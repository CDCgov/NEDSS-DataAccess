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
public class Race {
    private String raceCd;
    private String raceDescTxt;
    private String raceCategoryCd;
    @JsonProperty("person_uid")
    private Long personUid;

    public <T extends PersonExtendedProps> T updatePerson(T personFull) {
        personFull.setRaceCd(raceCd);
        personFull.setRaceCategory(raceCategoryCd);
        personFull.setRaceDesc(raceDescTxt);
        personFull.setPrPersonUid(personUid);
        return personFull;
    }
}
