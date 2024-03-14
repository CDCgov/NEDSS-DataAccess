package gov.cdc.etldatapipeline.person.model.dto.persondetail;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChgAuthUser {
    @JsonProperty("LAST_CHG_USER_ID")
    private Long lastChgUserId;
    @JsonProperty("add_time")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime addTime;
    @JsonProperty("LAST_CHG_USER_TIME")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime lastChgUserTime;
    @JsonProperty("PATIENT_LAST_UPDATED_BY")
    private String patientLastUpdatedBy;

    public <T extends PersonExtendedProps> T updatePerson(T personFull) {
        personFull.setLastChangedBy(lastChgUserId);
        return personFull;
    }
}
